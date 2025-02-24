import streamlit as st
import pandas as pd
import os
import sqlite3
from io import BytesIO
from pathlib import Path
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def send_validation_email(data_summary):
    """Send validation email with data summary"""
    email_sender = os.getenv('EMAIL_SENDER')
    email_password = os.getenv('EMAIL_PASSWORD')
    email_receiver = os.getenv('EMAIL_RECEIVER')

    if not all([email_sender, email_password, email_receiver]):
        st.error("Email configuration is missing. Please check your .env file.")
        return False

    try:
        subject = "✅ Data Validation Report"
        body = f"""
        Data Validation Report
        Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        Summary:
        {data_summary}
        
        This is an automated message confirming that the data has been validated.
        """
        
        msg = EmailMessage()
        msg["From"] = email_sender
        msg["To"] = email_receiver
        msg["Subject"] = subject
        msg.set_content(body)
        
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(email_sender, email_password)
            server.send_message(msg)
        return True
        
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return False

def store_sheets_in_db(file):
    try:
        excel_file = pd.ExcelFile(file)
        file_name = Path(file.name).stem
        output_dbs = {}

        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            db_name = f"{file_name}_{sheet_name}.db".replace(" ", "_").replace("-", "_")
            conn = sqlite3.connect(db_name)
            df.to_sql(sheet_name.replace(" ", "_"), conn, if_exists="replace", index=False)
            conn.close()
            output_dbs[sheet_name] = db_name
        
        return output_dbs
    except Exception as e:
        return {}

def fetch_data(db_paths):
    all_data = []
    
    for db_path in db_paths:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            query = f"""
            SELECT event, property, page, price_type, total_impressions, total_rate
            FROM "{table}"
            """
            try:
                df = pd.read_sql_query(query, conn)
                all_data.append(df)
            except:
                pass
        
        conn.close()
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.groupby(["event", "property", "page", "price_type"], as_index=False).sum()
        return combined_df
    else:
        return pd.DataFrame()

def generate_data_summary(event_summary, property_summary):
    """Generate summary for email report"""
    return f"""
    Event Summary:
    Total Events: {len(event_summary)}
    Total Impressions (Millions): {event_summary['total_imps (Millions)'].sum():.2f}
    Total Rate (Crores): {event_summary['total_rate (Crores)'].sum():.2f}
    
    Property Summary:
    Total Properties: {len(property_summary)}
    Total Impressions (Millions): {property_summary['total_imps (Millions)'].sum():.2f}
    Total Rate (Crores): {property_summary['total_rate (Crores)'].sum():.2f}
    """

# Streamlit UI
st.title("Upload Excel File")
uploaded_file = st.file_uploader("Upload", type=["xlsx"])

db_files = {}
if uploaded_file is not None:
    db_files = store_sheets_in_db(uploaded_file)
    
    if db_files:
        aggregated_data = fetch_data(list(db_files.values()))
        if not aggregated_data.empty:
            st.write("### Processed Data")
            st.dataframe(aggregated_data)

            # EVENT-WISE REPORT FEATURE
            unique_events = aggregated_data["event"].unique().tolist()
            selected_event = st.selectbox("Select an Event", ["All"] + unique_events)

            if selected_event != "All":
                event_data = aggregated_data[aggregated_data["event"] == selected_event]
            else:
                event_data = aggregated_data

            st.write("### Event-wise Report")
            st.dataframe(event_data)

            # Download Button for Event-wise Report
            output = BytesIO()
            event_data.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)

            st.download_button(
                label="Download Event Report",
                data=output,
                file_name=f"{selected_event}_report.xlsx" if selected_event != "All" else "aggregated_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # EVENT-WISE SUMMARY REPORT
            event_summary = aggregated_data.groupby("event", as_index=False).agg(
                total_imps=("total_impressions", "sum"),
                total_rate=("total_rate", "sum")
            )

            # Convert total_imps to millions and total_rate to crores
            event_summary["total_imps"] = event_summary["total_imps"] / 1_000_000
            event_summary["total_rate"] = event_summary["total_rate"] / 10_000_000
            event_summary.rename(columns={"total_imps": "total_imps (Millions)", "total_rate": "total_rate (Crores)"}, inplace=True)

            st.write("### Event-wise Summary Report")
            st.dataframe(event_summary)

            # Download Button for Event Summary Report
            summary_output = BytesIO()
            event_summary.to_excel(summary_output, index=False, engine='openpyxl')
            summary_output.seek(0)

            st.download_button(
                label="Download Event Summary",
                data=summary_output,
                file_name="event_summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # PROPERTY-WISE REPORT FEATURE
            unique_properties = aggregated_data["property"].unique().tolist()
            selected_property = st.selectbox("Select a Property", ["All"] + unique_properties)

            if selected_property != "All":
                property_data = aggregated_data[aggregated_data["property"] == selected_property]
            else:
                property_data = aggregated_data

            st.write("### Property-wise Report")
            st.dataframe(property_data)

            # Download Button for Property-wise Report
            property_output = BytesIO()
            property_data.to_excel(property_output, index=False, engine='openpyxl')
            property_output.seek(0)

            st.download_button(
                label="Download Property Report",
                data=property_output,
                file_name=f"{selected_property}_report.xlsx" if selected_property != "All" else "property_aggregated_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # PROPERTY-WISE SUMMARY REPORT
            property_summary = aggregated_data.groupby("property", as_index=False).agg(
                total_imps=("total_impressions", "sum"),
                total_rate=("total_rate", "sum")
            )

            # Convert total_imps to millions and total_rate to crores
            property_summary["total_imps"] = property_summary["total_imps"] / 1_000_000
            property_summary["total_rate"] = property_summary["total_rate"] / 10_000_000
            property_summary.rename(columns={"total_imps": "total_imps (Millions)", "total_rate": "total_rate (Crores)"}, inplace=True)

            st.write("### Property-wise Summary Report")
            st.dataframe(property_summary)

            # Download Button for Property Summary Report
            property_summary_output = BytesIO()
            property_summary.to_excel(property_summary_output, index=False, engine='openpyxl')
            property_summary_output.seek(0)

            st.download_button(
                label="Download Property Summary",
                data=property_summary_output,
                file_name="property_summary.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Add validation section
            st.write("### Data Validation")
            col1, col2 = st.columns([1, 2])
            
            with col1:
                if st.button("✅ Validate Data"):
                    data_summary = generate_data_summary(event_summary, property_summary)
                    if send_validation_email(data_summary):
                        st.success("✅ Data validated! Email notification sent successfully.")
                    else:
                        st.warning("⚠️ Validation completed but email notification failed.")
            
            with col2:
                st.info("""
                Click 'Validate Data' to confirm the processed data is correct. 
                This will send a validation report to the configured email address.
                """)

            # Cleanup database files
            for db_file in db_files.values():
                try:
                    os.remove(db_file)
                except:
                    pass