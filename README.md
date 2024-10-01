# Margefson ETL Process Monitoring Dashboard

This repository contains a Python-based **ETL (Extract, Transform, Load) process** that integrates **Streamlit** to create a user-friendly interface for monitoring the execution of the ETL pipeline in real-time.

### Key Features:
- **Database Integration**: Seamlessly connects to a MySQL database to insert data from a CSV file.
- **Lazy Evaluation & Batch Processing**: Processes large CSV files in chunks, optimizing memory usage, and batches database insertions for better performance.
- **Streamlit Dashboard**: Provides a web-based interface for users to:
  - Input MySQL credentials and upload CSV files.
  - Monitor the progress of the ETL process using real-time logs and a progress bar.
  - View error or success messages during execution.
  
### How It Works:
1. **Upload a CSV File**: Users can upload a CSV file directly from the Streamlit dashboard.
2. **Transform and Load**: The data from the CSV is transformed and inserted into the MySQL database.
3. **Progress Tracking**: Monitor each step of the ETL process via the progress bar and real-time feedback.

### Getting Started:
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/etl-dashboard.git
   ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the Streamlit app:
   ```bash
   streamlit run etl_dashboard.py
   ```

### Requirements:
- **Python 3.7+**
- **MySQL** installed and running
- Python dependencies listed in `requirements.txt`

### Future Improvements:
- Adding support for multiple database systems.
- Adding file validation and error reporting for malformed CSVs.
- Expanding to handle various file formats such as Excel or JSON.
