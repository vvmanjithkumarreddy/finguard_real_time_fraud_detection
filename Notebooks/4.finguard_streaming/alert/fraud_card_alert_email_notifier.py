from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def create_fraud_alert_email_body(alert_data):
    """Creates the HTML email body for a fraud card alert."""
    return f"""<html><body style="font-family: Arial, sans-serif;">
        <h2 style="color: #c9302c;">🚨 FRAUD ALERT - Immediate Action Required</h2>
        <p>Dear {alert_data['customer_name']},</p>
        <p style="color: #c9302c; font-weight: bold;">We detected a transaction using a credit card that has been flagged for fraud:</p>
        <div style="background-color: #f2dede; padding: 15px; border-left: 4px solid #c9302c; margin: 20px 0;">
        <h3 style="margin-top: 0; color: #a94442;">⚠️ Transaction Details</h3>
        <ul style="list-style-type: none; padding-left: 0;">
        <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
        <li><strong>Amount:</strong> {alert_data['currency']} {alert_data['transaction_amount']:,.2f}</li>
        <li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
        <li><strong>Transaction Type:</strong> {alert_data['transaction_type']}</li>
        <li><strong>Payment Channel:</strong> {alert_data['payment_channel']}</li>
        <li><strong>Location:</strong> {alert_data['transaction_city']}, {alert_data['transaction_country']}</li>
        <li><strong>International:</strong> {"Yes" if alert_data['is_international'] else "No"}</li>
        <li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
        <li><strong>Status:</strong> {alert_data['transaction_status']}</li>
        </ul></div>
        <div style="background-color: #fcf8e3; padding: 15px; border-left: 4px solid #f0ad4e; margin: 20px 0;">
        <h3 style="margin-top: 0; color: #8a6d3b;">🔍 Fraud Watchlist Information</h3>
        <ul style="list-style-type: none; padding-left: 0;">
        <li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
        <li><strong>Risk Level:</strong> <span style="color: #c9302c; font-weight: bold;">{alert_data['risk_level']}</span></li>
        <li><strong>Reason:</strong> {alert_data['reason_description']}</li>
        <li><strong>Reason Code:</strong> {alert_data['reason_code']}</li>
        <li><strong>Reported By:</strong> {alert_data['reported_by']} ({alert_data['reported_source']})</li>
        <li><strong>Watchlist Type:</strong> {alert_data['watchlist_type']}</li>
        <li><strong>Action:</strong> {alert_data['action']}</li>
        </ul></div>
        <p style="background-color: #d9534f; color: white; padding: 15px; border-radius: 5px;">
        <strong>🛑 IMMEDIATE ACTION REQUIRED:</strong><br>
        • If you did NOT authorize this transaction, contact our fraud department immediately at 1-800-FRAUD-HELP<br>
        • Your card may have been compromised and should be blocked immediately<br>
        • Do not respond to any suspicious emails or calls requesting your card details<br>
        • Review your recent transactions for any other unauthorized activity
        </p>
        <p>For your protection, we have flagged this transaction for review. Our fraud team will contact you within 24 hours.</p>
        <hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
        <p style="font-size: 12px; color: #6c757d;">
        Alert ID: {alert_data['alert_id']}<br>
        Alert Type: {alert_data['alert_type']}<br>
        Alert Timestamp: {alert_data['alert_timestamp']}<br>
        This is an automated message from FinGuard Fraud Detection System.
        </p>
        </body></html>"""


# Get configuration outside the foreach_batch_sink for serialization
EMAIL_FROM = "mkresds@gmail.com"
try:
    APP_PASSWORD = dbutils.secrets.get("finguard-scope", "gmail_api_key")
except Exception as e:
    print(f"❌ Failed to retrieve Gmail API key from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for fraud card transactions."""
    
    if APP_PASSWORD is None:
        print(f"❌ Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return
    
    rows = df.collect()
    print(f"🚨 Batch {batch_id}: Processing {len(rows)} fraud alert(s)...")
    
    success_count = 0
    failure_count = 0
    
    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'alert_type': row.alert_type,
                'alert_timestamp': str(row.alert_timestamp),
                'transaction_id': row.transaction_id,
                'customer_name': row.customer_name,
                'transaction_amount': float(row.transaction_amount),
                'currency': row.currency,
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_type': row.transaction_type,
                'payment_channel': row.payment_channel,
                'transaction_city': row.transaction_city,
                'transaction_country': row.transaction_country,
                'is_international': row.is_international,
                'transaction_timestamp': str(row.transaction_timestamp),
                'transaction_status': row.transaction_status,
                'watchlist_id': row.watchlist_id,
                'watchlist_type': row.watch_type,
                'risk_level': row.risk_level,
                'action': row.action,
                'reason_code': row.reason_code,
                'reason_description': row.reason_description,
                'reported_by': row.reported_by,
                'reported_source': row.reported_source
            }
            
            subject = f"🚨 FRAUD ALERT - {alert_data['alert_id']} - Immediate Action Required"
            body = create_fraud_alert_email_body(alert_data)
            
            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)
            
            success_count += 1
            print(f"  ✅ Fraud alert email sent to {row.customer_email} for transaction {row.transaction_id}")
            
        except Exception as e:
            failure_count += 1
            print(f"  ❌ Error processing fraud alert {row.alert_id}: {e}")
    
    print(f"📊 Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")


@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_card_alert_stream():
    """Streaming flow that reads fraud card alerts."""
    return spark.readStream.table("finguard.gold.fraud_card_alert")