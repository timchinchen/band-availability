from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_cors import CORS
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Allow OAuth over HTTP for local development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from typing import List, Literal

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')

# Production security settings
if os.getenv('RENDER') or os.getenv('HEROKU'):
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

CORS(app)

# Google OAuth Configuration
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_CONFIG = json.loads(os.environ["CLIENT_CONFIG"])

# Environment variables
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')

# Pydantic models for structured output
class AvailabilityUpdate(BaseModel):
    dates: List[str] = Field(description="List of dates in YYYY-MM-DD format")
    status: Literal["available", "unavailable"] = Field(
        description="Whether the member is available or unavailable"
    )

# Initialize LangChain
llm = ChatOpenAI(
    model="gpt-4o-mini",
    api_key=OPENAI_API_KEY,
    temperature=0
)

parser = PydanticOutputParser(pydantic_object=AvailabilityUpdate)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are an AI assistant that parses natural language availability statements into structured data.
Today's date is {today}.

Extract:
1. All specific dates mentioned (convert to YYYY-MM-DD format)
2. Whether the person is available or unavailable for those dates

Common patterns:
- "I can't do May 5th" = unavailable on 2025-05-05
- "I'm available May 12" = available on 2025-05-12
- "Not available next Tuesday" = calculate the date of next Tuesday from today
- "Available all of June" = generate all dates in June
- "Can't do the 5th" = assume current or next month depending on context

Important: When parsing relative dates like "next Tuesday" or "the 5th", calculate from today's date.

{format_instructions}"""),
    ("human", "{availability_text}")
])


def get_sheets_service():
    """Get authenticated Google Sheets service"""
    if 'credentials' not in session:
        return None
    
    credentials = Credentials(**session['credentials'])
    return build('sheets', 'v4', credentials=credentials)


def parse_availability(availability_text: str) -> AvailabilityUpdate:
    """Use LangChain to parse natural language availability"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    chain = prompt | llm | parser
    
    result = chain.invoke({
        "availability_text": availability_text,
        "today": today,
        "format_instructions": parser.get_format_instructions()
    })
    
    return result


def update_google_sheet(member_name: str, dates: List[str], status: str):
    """Update Google Sheet with availability"""
    service = get_sheets_service()
    if not service:
        raise Exception("Not authenticated with Google")
    
    # Get sheet name dynamically
    sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', [])
    if not sheets:
        raise Exception("No sheets found in spreadsheet")
    
    sheet_name = sheets[0]['properties']['title']
    print(f"Updating sheet: {sheet_name}")
    
    # Get current sheet data
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{sheet_name}!A1:Z1000'
    ).execute()
    
    values = result.get('values', [])
    
    if not values:
        raise Exception("Sheet is empty")
    
    # Find member column
    headers = values[0]
    try:
        member_col_index = headers.index(member_name)
    except ValueError:
        raise Exception(f"Member '{member_name}' not found. Available: {', '.join(headers[1:])}")
    
    # Update dates
    updates = []
    dates_not_found = []
    
    for date in dates:
        found = False
        for row_index, row in enumerate(values[1:], start=2):
            if len(row) > 0 and row[0] == date:
                col_letter = chr(65 + member_col_index)
                cell = f"{col_letter}{row_index}"
                
                updates.append({
                    'range': f'{sheet_name}!{cell}',
                    'values': [['✓' if status == 'available' else '✗']]
                })
                found = True
                break
        
        if not found:
            dates_not_found.append(date)
    
    if updates:
        body = {'data': updates, 'valueInputOption': 'RAW'}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()
    
    return len(updates), dates_not_found


@app.route('/')
def index():
    """Main page"""
    if 'credentials' not in session:
        return redirect(url_for('authorize'))
    
    return render_template('index.html')


@app.route('/authorize')
def authorize():
    """Start OAuth flow"""
    print("=== STARTING OAUTH FLOW ===")
    flow = Flow.from_client_config(
        CLIENT_CONFIG,
        scopes=SCOPES,
        redirect_uri=url_for('oauth2callback', _external=True)
    )
    
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true'
    )
    
    print(f"Redirect URI: {url_for('oauth2callback', _external=True)}")
    print(f"Authorization URL: {authorization_url}")
    
    session['state'] = state
    return redirect(authorization_url)


@app.route('/oauth2callback')
def oauth2callback():
    """OAuth callback"""
    print("=== OAUTH CALLBACK RECEIVED ===")
    print(f"Request URL: {request.url}")
    print(f"Session state: {session.get('state', 'NOT FOUND')}")
    
    state = session['state']
    
    flow = Flow.from_client_config(
        CLIENT_CONFIG,
        scopes=SCOPES,
        state=state,
        redirect_uri=url_for('oauth2callback', _external=True)
    )
    
    flow.fetch_token(authorization_response=request.url)
    
    credentials = flow.credentials
    session['credentials'] = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    
    print("OAuth credentials stored in session")
    print(f"Credentials: token={credentials.token[:20]}...")
    
    return redirect(url_for('index'))


@app.route('/logout')
def logout():
    """Clear session"""
    session.clear()
    return redirect(url_for('index'))


@app.route('/api/members', methods=['GET'])
def get_members():
    """Get list of band members from sheet"""
    try:
        print("=== GET MEMBERS REQUEST ===")
        print(f"Session credentials present: {'credentials' in session}")
        
        service = get_sheets_service()
        if not service:
            print("ERROR: Not authenticated")
            return jsonify({'error': 'Not authenticated'}), 401
        
        print(f"Fetching from spreadsheet: {SPREADSHEET_ID}")
        
        # First, get the sheet metadata to find the actual sheet name
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', [])
        print(f"Available sheets: {[s['properties']['title'] for s in sheets]}")
        
        # Use the first sheet's name
        if not sheets:
            return jsonify({'error': 'No sheets found in spreadsheet'}), 404
        
        sheet_name = sheets[0]['properties']['title']
        print(f"Using sheet: {sheet_name}")
        
        # Fetch the first row
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:Z1'
        ).execute()
        
        print(f"API Response: {result}")
        
        headers = result.get('values', [[]])[0]
        print(f"Headers found: {headers}")
        
        members = headers[1:] if len(headers) > 1 else []
        print(f"Members extracted: {members}")
        
        return jsonify({'members': members})
    
    except Exception as e:
        print(f"ERROR in get_members: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/update-availability', methods=['POST'])
def update_availability():
    """Update availability endpoint"""
    try:
        data = request.json
        member_name = data.get('memberName')
        availability_text = data.get('availabilityText')
        
        if not member_name or not availability_text:
            return jsonify({'error': 'Missing memberName or availabilityText'}), 400
        
        if 'credentials' not in session:
            return jsonify({'error': 'Not authenticated'}), 401
        
        # Parse with LangChain
        parsed = parse_availability(availability_text)
        
        # Update sheet
        updated_count, dates_not_found = update_google_sheet(
            member_name=member_name,
            dates=parsed.dates,
            status=parsed.status
        )
        
        message = f'Updated {updated_count} date(s) successfully'
        if dates_not_found:
            message += f'. Dates not found in sheet: {", ".join(dates_not_found)}'
        
        return jsonify({
            'message': message,
            'dates': parsed.dates,
            'status': parsed.status,
            'updated_count': updated_count,
            'dates_not_found': dates_not_found
        })
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/view-schedule', methods=['GET'])
def view_schedule():
    """Get current schedule"""
    try:
        print("=== VIEW SCHEDULE REQUEST ===")
        service = get_sheets_service()
        if not service:
            return jsonify({'error': 'Not authenticated'}), 401
        
        # Get sheet name dynamically
        sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        sheets = sheet_metadata.get('sheets', [])
        if not sheets:
            return jsonify({'error': 'No sheets found'}), 404
        
        sheet_name = sheets[0]['properties']['title']
        print(f"Fetching schedule from sheet: {sheet_name}")
        
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:Z100'
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            return jsonify({'error': 'Sheet is empty'}), 404
        
        print(f"Found {len(values)} rows")
        return jsonify({'schedule': values})
    
    except Exception as e:
        print(f"ERROR in view_schedule: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    if not SPREADSHEET_ID:
        print("WARNING: SPREADSHEET_ID environment variable not set!")
    else:
        print(f"Using spreadsheet: {SPREADSHEET_ID}")
    
    if not OPENAI_API_KEY:
        print("WARNING: OPENAI_API_KEY environment variable not set!")
    else:
        print("OpenAI API key loaded successfully")
    
    print("\n=== Starting Flask app on port 5001 ===\n")
    app.run(debug=True, host='0.0.0.0', port=5001)