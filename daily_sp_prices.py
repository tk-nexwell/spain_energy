import os
import requests
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
import logging

# Constants
BASE_URL = "https://www.omie.es/en/file-download"
SOURCE_PAGE_URL = "https://www.omie.es/en/file-access-list?parents=/Day-ahead%20Market/1.%20Prices"
SPAIN_FOLDER = "C:\\Users\\ThomasKoenig\\spain"
OUTPUT_FOLDER = os.path.join(SPAIN_FOLDER, "TestReport")
ARCHIVE_FOLDER = os.path.join(SPAIN_FOLDER, "EmailArchive")
LOG_FILE = os.path.join(SPAIN_FOLDER, "log.txt")
BCC_FILE = os.path.join(SPAIN_FOLDER, "bcc_list.txt")
EMAIL_FROM = "tomakoenig@gmail.com"
EMAIL_FROM_NAME = "Thomas Koenig"
EMAIL_PASSWORD = "zrqp zgvd jhfa qtoi"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_TO = "thomas@nexwell.com"

# Setup folders
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.info("Starting daily email task.")

try:
    # Read Bcc email list
    if not os.path.exists(BCC_FILE):
        raise FileNotFoundError(f"Bcc file '{BCC_FILE}' not found!")
    with open(BCC_FILE, "r") as f:
        bcc_list = [line.strip() for line in f if line.strip()]
    if not bcc_list:
        raise ValueError("No emails found in Bcc file!")
    logging.info(f"Bcc list loaded: {bcc_list}")

    # 1. Download file for yesterday
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y%m%d")
    formatted_date = yesterday.strftime("%A %d %B %Y")
    file_name = f"marginalpdbc_{yesterday_str}.1"
    file_url = f"{BASE_URL}?parents=marginalpdbc&filename={file_name}"
    file_path = os.path.join(OUTPUT_FOLDER, file_name)

    logging.info(f"Attempting to download: {file_url}")
    response = requests.get(file_url)

    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        logging.info(f"File downloaded successfully: {file_path}")
    else:
        raise Exception(f"Failed to download file: {file_url} (Status: {response.status_code})")

    # Check if the file is valid (not HTML)
    with open(file_path, "r") as f:
        content = f.read()
    if "<html" in content.lower():
        raise Exception(f"The file downloaded from {file_url} is an HTML file, not a data file.")

    # 2. Process the file and generate a graph
    with open(file_path, "r") as f:
        lines = f.readlines()[1:-1]

    data = [line.strip(";").split(";")[:6] for line in lines]
    df = pd.DataFrame(data, columns=["Year", "Month", "Day", "Hour", "Price1", "Price2"])
    df = df.astype({"Year": int, "Month": int, "Day": int, "Hour": int, "Price1": float, "Price2": float})

    high = df["Price1"].max()
    low = df["Price1"].min()
    average = df["Price1"].mean()
    spread = high - low

    logging.info(f"Summary statistics - High: {high}, Low: {low}, Average: {average}, Spread: {spread}")

    # Generate a graph
    plt.figure(figsize=(10, 6))
    plt.plot(df["Hour"], df["Price1"], marker="o", label="Price (€)")
    plt.title(f"Electricity Prices for {formatted_date}")
    plt.xlabel("Hour")
    plt.ylabel("Price (€)")
    plt.xticks(range(1, 25))
    plt.grid()
    plt.legend()
    graph_path = os.path.join(OUTPUT_FOLDER, f"graph_{yesterday_str}.png")
    plt.savefig(graph_path)
    plt.close()
    logging.info(f"Graph saved: {graph_path}")

    # Generate an Excel file
    excel_path = os.path.join(OUTPUT_FOLDER, f"hourly_data_{yesterday_str}.xlsx")
    df.to_excel(excel_path, index=False)
    logging.info(f"Excel file saved: {excel_path}")

    # Generate table in HTML format
    df_table = df[["Hour", "Price1", "Price2"]].to_html(index=False, justify="center")

    # Generate email content
    email_subject = f"{formatted_date} hourly electricity prices Spain"
    email_body = f"""
    <p>Good morning,</p>
    <p>Here is the electricity price report for <b>{formatted_date}</b>:</p>
    <img src="cid:graph" alt="Graph" style="width:600px; height:auto;">
    <p>The summary statistics are as follows:</p>
    <ul>
        <li><b>High Price:</b> €{high:.2f}</li>
        <li><b>Low Price:</b> €{low:.2f}</li>
        <li><b>Average Price:</b> €{average:.2f}</li>
        <li><b>Spread (Max - Min):</b> €{spread:.2f}</li>
    </ul>
    <p>View the <a href="{SOURCE_PAGE_URL}">hourly data</a>.</p>
    <p>Detailed hourly data:</p>
    {df_table}
    <p>Best regards,</p>
    <p>Thomas Koenig</p>
    """

    # Create email
    msg = MIMEMultipart()
    msg["From"] = formataddr((EMAIL_FROM_NAME, EMAIL_FROM))
    msg["To"] = EMAIL_TO
    msg["Bcc"] = ", ".join(bcc_list)
    msg["Subject"] = email_subject

    # Attach HTML body
    msg.attach(MIMEText(email_body, "html"))

    # Attach graph to email (embedded)
    with open(graph_path, "rb") as img:
        msg_image = MIMEImage(img.read(), name=os.path.basename(graph_path))
        msg_image.add_header("Content-ID", "<graph>")
        msg.attach(msg_image)

    # Attach graph as a file
    with open(graph_path, "rb") as graph_file:
        graph_attachment = MIMEBase("application", "octet-stream")
        graph_attachment.set_payload(graph_file.read())
        encoders.encode_base64(graph_attachment)
        graph_attachment.add_header(
            "Content-Disposition", f"attachment; filename={os.path.basename(graph_path)}"
        )
        msg.attach(graph_attachment)

    # Attach Excel file
    with open(excel_path, "rb") as excel_file:
        excel_attachment = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        excel_attachment.set_payload(excel_file.read())
        encoders.encode_base64(excel_attachment)
        excel_attachment.add_header(
            "Content-Disposition", f"attachment; filename={os.path.basename(excel_path)}"
        )
        msg.attach(excel_attachment)

    # Send email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, [EMAIL_TO] + bcc_list, msg.as_string())
    logging.info("Email sent successfully.")
    print("Email sent successfully.")

except Exception as e:
    # Log the error
    logging.error(f"Script failed: {e}")
    print(f"An error occurred: {e}")
