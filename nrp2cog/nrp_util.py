from oauth2client.service_account import ServiceAccountCredentials
import gspread

def get_google_session(credentials='credentials.json'):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials, scope)
    client = gspread.authorize(creds)
    return client