# Download SFTP files

1. Create the environment variables on the host

    SFTP_HOSTNAME 
    SFTP_USERNAME
    SFTP_PASSWORD
    REMOTE_PATH
    LOCAL_FOLDER

2. Create and activated an virtual environment
   python -m venv venv
   venv/Scripts/activate

3. Install requeriment dependencies
   pip install -r requirements.txt
   
4. Created a task scheduler and add the download-sftp.py