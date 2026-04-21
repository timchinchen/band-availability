from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from flask_cors import CORS
import os
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Allow OAuth over HTTP for local development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
import re
from openai import OpenAI

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-change-this')

# Production security settings
if os.getenv('RENDER') or os.getenv('HEROKU'):
    app.config['SESSION_COOKIE_SECURE'] = True
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

CORS(app)

# Keep Google API client behavior lightweight for low-memory hosts.
os.environ.setdefault('GOOGLE_API_USE_MTLS_ENDPOINT', 'never')

# Google OAuth Configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/calendar.readonly'
]
CLIENT_CONFIG = json.loads(os.environ["CLIENT_CONFIG"])

# Environment variables
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4o-mini')
CALENDAR_IDS = [
    calendar_id.strip()
    for calendar_id in os.environ.get('CALENDAR_IDS', os.environ.get('CALENDAR_ID', 'primary')).split(',')
    if calendar_id.strip()
]

SCHEDULE_LOOKBACK_DAYS = 14
SCHEDULE_LOOKAHEAD_DAYS = 120
MAX_SCHEDULE_ROWS = 180
KEY_EVENT_LIMIT = 20
try:
    CALENDAR_RETRY_BACKOFF_MINUTES = int(os.environ.get('CALENDAR_RETRY_BACKOFF_MINUTES', '60'))
except ValueError:
    CALENDAR_RETRY_BACKOFF_MINUTES = 60
CALENDAR_RETRY_BACKOFF_MINUTES = max(5, min(1440, CALENDAR_RETRY_BACKOFF_MINUTES))
CALENDAR_API_ENABLED = os.environ.get('CALENDAR_API_ENABLED', 'true').strip().lower() not in {'0', 'false', 'no'}

EXCLUDED_MEMBER_HEADERS = {'date', 'event', 'amount', 'notes', 'venue'}

AVAILABILITY_PARSE_PROMPT = """You parse natural language availability statements into JSON.
Today's date is {today}.

Extract:
1) dates: array of all dates mentioned in YYYY-MM-DD format.
2) status: "available" or "unavailable".

Rules:
- Resolve relative dates from today's date.
- Return only strict JSON object: {{"dates": [...], "status": "available|unavailable"}}.
- Do not include explanation or markdown."""

_openai_client: Optional[OpenAI] = None
_calendar_retry_after: Optional[datetime] = None
_calendar_unavailable_reason: Optional[str] = None


@dataclass
class AvailabilityUpdate:
    dates: List[str]
    status: str


def get_openai_client() -> OpenAI:
    """Lazily initialize OpenAI client to keep startup memory lower"""
    global _openai_client
    if _openai_client is None:
        if not OPENAI_API_KEY:
            raise Exception("OPENAI_API_KEY environment variable is not set")
        _openai_client = OpenAI(api_key=OPENAI_API_KEY)
    return _openai_client


def normalize_availability_payload(payload: Dict[str, Any]) -> AvailabilityUpdate:
    """Validate and normalize LLM JSON payload"""
    status = str(payload.get("status", "")).strip().lower()
    if status not in {"available", "unavailable"}:
        raise ValueError("Could not determine availability status")

    raw_dates = payload.get("dates", [])
    if not isinstance(raw_dates, list):
        raise ValueError("Expected 'dates' to be a list")

    normalized_dates: List[str] = []
    seen = set()
    for raw_date in raw_dates:
        if not isinstance(raw_date, str):
            continue
        date_value = raw_date.strip()
        try:
            datetime.strptime(date_value, "%Y-%m-%d")
        except ValueError:
            continue
        if date_value not in seen:
            seen.add(date_value)
            normalized_dates.append(date_value)

    if not normalized_dates:
        raise ValueError("No valid dates detected in availability update")

    return AvailabilityUpdate(dates=normalized_dates, status=status)


def is_calendar_disabled() -> Tuple[bool, Optional[str]]:
    """Check whether calendar calls should be skipped for now"""
    global _calendar_retry_after
    now = datetime.now(timezone.utc)

    if not CALENDAR_API_ENABLED:
        return True, "Calendar integration disabled by CALENDAR_API_ENABLED"

    if _calendar_retry_after and now < _calendar_retry_after:
        minutes_remaining = int((_calendar_retry_after - now).total_seconds() // 60) + 1
        reason = _calendar_unavailable_reason or "Calendar temporarily unavailable"
        return True, f"{reason}. Retrying in about {minutes_remaining} minute(s)"

    return False, None


def disable_calendar_temporarily(reason: str):
    """Back off calendar calls after known API/config failures"""
    global _calendar_retry_after, _calendar_unavailable_reason
    _calendar_unavailable_reason = reason
    _calendar_retry_after = datetime.now(timezone.utc) + timedelta(minutes=CALENDAR_RETRY_BACKOFF_MINUTES)


def should_disable_calendar_after_error(error: Exception) -> Optional[str]:
    """Map Google Calendar API errors to a backoff reason"""
    if isinstance(error, HttpError):
        status_code = getattr(error.resp, "status", None)
        message = str(error).lower()

        if status_code == 403 and (
            "accessnotconfigured" in message or
            "has not been used in project" in message or
            "api has not been used" in message
        ):
            return "Google Calendar API is not enabled for this project"
        if status_code == 403:
            return "Google Calendar access forbidden"
        if status_code == 429:
            return "Google Calendar quota exceeded"
        if status_code in {500, 503}:
            return "Google Calendar service unavailable"

    return None


def filter_member_headers(headers: List[str]) -> List[str]:
    """Return only real member columns, excluding metadata columns"""
    members: List[str] = []
    for header in headers[1:]:
        cleaned = header.strip()
        if not cleaned:
            continue
        if cleaned.lower() in EXCLUDED_MEMBER_HEADERS:
            continue
        members.append(cleaned)
    return members


def find_member_column_index(headers: List[str], member_name: str) -> int:
    """Find a member column by trimmed, case-insensitive header match"""
    normalized_target = member_name.strip().lower()
    for index, header in enumerate(headers):
        if header.strip().lower() == normalized_target:
            return index
    raise ValueError(member_name)


def get_sheets_service():
    """Get authenticated Google Sheets service"""
    if 'credentials' not in session:
        return None
    
    credentials = Credentials(**session['credentials'])
    return build('sheets', 'v4', credentials=credentials, cache_discovery=False)


def get_calendar_service():
    """Get authenticated Google Calendar service"""
    if 'credentials' not in session:
        return None
    
    credentials = Credentials(**session['credentials'])
    return build('calendar', 'v3', credentials=credentials, cache_discovery=False)


def get_primary_sheet_name(service) -> str:
    """Get the first sheet name from the spreadsheet"""
    sheet_metadata = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = sheet_metadata.get('sheets', [])
    if not sheets:
        raise Exception("No sheets found in spreadsheet")
    return sheets[0]['properties']['title']


def to_int(value: Optional[str], default: int, min_value: int = 1, max_value: int = 500) -> int:
    """Safely parse an integer query parameter"""
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return max(min_value, min(parsed, max_value))


def parse_sheet_date(raw_value: str) -> Optional[datetime.date]:
    """Parse date strings from spreadsheet rows"""
    if not raw_value:
        return None
    
    value = raw_value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%b %d %Y", "%d %b %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def normalize_row(row: List[str], width: int) -> List[str]:
    """Pad/truncate sheet rows so they align with table headers"""
    normalized = list(row[:width])
    if len(normalized) < width:
        normalized.extend([''] * (width - len(normalized)))
    return normalized


def build_schedule_window(
    values: List[List[str]],
    lookback_days: int = SCHEDULE_LOOKBACK_DAYS,
    lookahead_days: int = SCHEDULE_LOOKAHEAD_DAYS,
    max_rows: int = MAX_SCHEDULE_ROWS
) -> List[List[str]]:
    """Return a date-windowed subset of schedule rows for a representative view"""
    if not values:
        return []
    
    headers = values[0]
    if not headers:
        return values
    
    today = datetime.now().date()
    window_start = today - timedelta(days=lookback_days)
    window_end = today + timedelta(days=lookahead_days)
    
    dated_rows: List[Tuple[datetime.date, List[str]]] = []
    fallback_dated_rows: List[Tuple[datetime.date, List[str]]] = []
    undated_rows: List[List[str]] = []
    
    for row in values[1:]:
        normalized_row = normalize_row(row, len(headers))
        row_date = parse_sheet_date(normalized_row[0] if normalized_row else '')
        
        if row_date is None:
            if any(cell.strip() for cell in normalized_row):
                undated_rows.append(normalized_row)
            continue
        
        fallback_dated_rows.append((row_date, normalized_row))
        if window_start <= row_date <= window_end:
            dated_rows.append((row_date, normalized_row))
    
    source_rows = dated_rows if dated_rows else fallback_dated_rows
    source_rows.sort(key=lambda item: item[0])
    
    selected_rows = [row for _, row in source_rows[:max_rows]]
    
    remaining_capacity = max(0, max_rows - len(selected_rows))
    if remaining_capacity > 0 and undated_rows:
        selected_rows.extend(undated_rows[:remaining_capacity])
    
    return [headers] + selected_rows


def detect_event_type(text: str) -> Optional[str]:
    """Classify event text into key event categories"""
    if not text:
        return None
    
    lowered = text.lower()
    if re.search(r"\b(rehearsal|practice|run[- ]through|soundcheck)\b", lowered):
        return "rehearsal"
    if re.search(r"\b(gig|show|festival|wedding|party|performance|concert|function|private event)\b", lowered):
        return "gig"
    return None


def extract_time_text(text: str) -> str:
    """Extract a readable time range from event text"""
    if not text:
        return ""
    
    range_match = re.search(
        r"(\d{1,2}(?::\d{2})?\s?(?:am|pm)?)\s*(?:-|–|to)\s*(\d{1,2}(?::\d{2})?\s?(?:am|pm)?)",
        text,
        flags=re.IGNORECASE
    )
    if range_match:
        start, end = range_match.groups()
        return f"{start.strip()} - {end.strip()}"
    
    single_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s?(?:am|pm))\b", text, flags=re.IGNORECASE)
    if single_match:
        return single_match.group(1).strip()
    
    return ""


def summarize_sheet_key_events(values: List[List[str]], limit: int = KEY_EVENT_LIMIT) -> List[Dict[str, str]]:
    """Extract rehearsal and gig events from spreadsheet EVENT column"""
    if not values:
        return []
    
    headers = values[0]
    if not headers:
        return []
    
    event_index = None
    for index, header in enumerate(headers):
        if header.strip().lower() == "event":
            event_index = index
            break
    
    if event_index is None:
        return []
    
    extracted_events: List[Dict[str, str]] = []
    for row in values[1:]:
        normalized_row = normalize_row(row, len(headers))
        event_text = normalized_row[event_index].strip() if event_index < len(normalized_row) else ""
        if not event_text:
            continue
        
        event_type = detect_event_type(event_text)
        if not event_type:
            continue
        
        event_date = parse_sheet_date(normalized_row[0])
        if not event_date:
            continue
        
        extracted_events.append({
            "date": event_date.isoformat(),
            "time": extract_time_text(event_text),
            "title": event_text,
            "type": event_type,
            "source": "sheet"
        })
    
    extracted_events.sort(key=lambda item: item["date"])
    return extracted_events[:limit]


def parse_google_datetime(raw_value: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime values returned by Google Calendar API"""
    if not raw_value:
        return None
    
    value = raw_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def format_calendar_event(event: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """Normalize a Google Calendar event into a key event record"""
    summary = (event.get("summary") or "").strip()
    description = (event.get("description") or "").strip()
    combined_text = f"{summary} {description}".strip()
    event_type = detect_event_type(combined_text)
    if not event_type:
        return None
    
    start = event.get("start", {})
    end = event.get("end", {})
    
    if "dateTime" in start:
        start_dt = parse_google_datetime(start.get("dateTime"))
        end_dt = parse_google_datetime(end.get("dateTime"))
        if not start_dt:
            return None
        date_value = start_dt.date().isoformat()
        if end_dt:
            time_value = f"{start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')}"
        else:
            time_value = start_dt.strftime('%H:%M')
    else:
        date_value = start.get("date")
        if not date_value:
            return None
        time_value = "All day"
    
    title = summary or "Untitled event"
    return {
        "date": date_value,
        "time": time_value,
        "title": title,
        "type": event_type,
        "source": "calendar"
    }


def summarize_calendar_key_events(calendar_service, limit: int = KEY_EVENT_LIMIT) -> List[Dict[str, str]]:
    """Fetch and summarize rehearsal/gig events from Google Calendar"""
    now = datetime.now(timezone.utc)
    time_min = (now - timedelta(days=SCHEDULE_LOOKBACK_DAYS)).isoformat()
    time_max = (now + timedelta(days=SCHEDULE_LOOKAHEAD_DAYS)).isoformat()
    
    events: List[Dict[str, str]] = []
    for calendar_id in CALENDAR_IDS:
        page_token = None
        while True:
            response = calendar_service.events().list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                maxResults=250,
                singleEvents=True,
                orderBy='startTime',
                pageToken=page_token
            ).execute()
            
            for event in response.get("items", []):
                normalized_event = format_calendar_event(event)
                if normalized_event:
                    events.append(normalized_event)
            
            page_token = response.get("nextPageToken")
            if not page_token:
                break
    
    events.sort(key=lambda item: (item["date"], item["time"], item["title"].lower()))
    
    deduped_events: List[Dict[str, str]] = []
    seen = set()
    for event in events:
        key = (event["date"], event["title"].lower(), event["type"])
        if key in seen:
            continue
        seen.add(key)
        deduped_events.append(event)
        if len(deduped_events) >= limit:
            break
    
    return deduped_events


def combine_key_events(
    calendar_events: List[Dict[str, str]],
    sheet_events: List[Dict[str, str]],
    limit: int = KEY_EVENT_LIMIT
) -> List[Dict[str, str]]:
    """Merge key events from calendar and sheet, deduplicated by date+title+type"""
    combined = calendar_events + sheet_events
    combined.sort(key=lambda item: (item["date"], item["time"], item["title"].lower()))
    
    deduped: List[Dict[str, str]] = []
    seen = set()
    for event in combined:
        key = (event["date"], event["title"].lower(), event["type"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)
        if len(deduped) >= limit:
            break
    
    return deduped


def parse_availability(availability_text: str) -> AvailabilityUpdate:
    """Use OpenAI JSON mode to parse natural language availability"""
    today = datetime.now().strftime("%Y-%m-%d")
    client = get_openai_client()

    completion = client.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": AVAILABILITY_PARSE_PROMPT.format(today=today)},
            {"role": "user", "content": availability_text}
        ]
    )

    content = (completion.choices[0].message.content or "").strip()
    if not content:
        raise Exception("Availability parser returned an empty response")

    try:
        payload = json.loads(content)
    except json.JSONDecodeError as json_error:
        raise Exception("Availability parser returned invalid JSON") from json_error

    return normalize_availability_payload(payload)


def update_google_sheet(member_name: str, dates: List[str], status: str):
    """Update Google Sheet with availability"""
    service = get_sheets_service()
    if not service:
        raise Exception("Not authenticated with Google")
    
    # Get sheet name dynamically
    sheet_name = get_primary_sheet_name(service)
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
        member_col_index = find_member_column_index(headers, member_name)
    except ValueError:
        raise Exception(
            f"Member '{member_name}' not found. "
            f"Available: {', '.join(filter_member_headers(headers))}"
        )
    
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
        sheet_name = get_primary_sheet_name(service)
        print(f"Using sheet: {sheet_name}")
        
        # Fetch the first row
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:Z1'
        ).execute()
        
        print(f"API Response: {result}")
        
        headers = result.get('values', [[]])[0]
        print(f"Headers found: {headers}")
        
        members = filter_member_headers(headers) if len(headers) > 1 else []
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
        
        # Parse availability text with OpenAI
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
        
        lookback_days = to_int(request.args.get('lookbackDays'), SCHEDULE_LOOKBACK_DAYS, 1, 90)
        lookahead_days = to_int(request.args.get('lookaheadDays'), SCHEDULE_LOOKAHEAD_DAYS, 7, 365)
        max_rows = to_int(request.args.get('maxRows'), MAX_SCHEDULE_ROWS, 10, 500)
        
        # Get sheet name dynamically
        sheet_name = get_primary_sheet_name(service)
        print(f"Fetching schedule from sheet: {sheet_name}")
        
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:Z1000'
        ).execute()
        
        raw_values = result.get('values', [])
        
        if not raw_values:
            return jsonify({'error': 'Sheet is empty'}), 404
        
        schedule = build_schedule_window(
            raw_values,
            lookback_days=lookback_days,
            lookahead_days=lookahead_days,
            max_rows=max_rows
        )
        
        print(f"Found {len(raw_values)} rows, returning {len(schedule) - 1} windowed rows")
        return jsonify({
            'schedule': schedule,
            'window': {
                'lookback_days': lookback_days,
                'lookahead_days': lookahead_days,
                'max_rows': max_rows
            },
            'rows_returned': max(0, len(schedule) - 1),
            'rows_total': max(0, len(raw_values) - 1)
        })
    
    except Exception as e:
        print(f"ERROR in view_schedule: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/key-events', methods=['GET'])
def key_events():
    """Get summarized rehearsal and gig events from calendar + sheet"""
    try:
        service = get_sheets_service()
        if not service:
            return jsonify({'error': 'Not authenticated'}), 401
        
        limit = to_int(request.args.get('limit'), KEY_EVENT_LIMIT, 1, 100)
        sheet_name = get_primary_sheet_name(service)
        
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A1:Z1000'
        ).execute()
        values = result.get('values', [])
        
        sheet_events = summarize_sheet_key_events(values, limit=limit)
        calendar_events: List[Dict[str, str]] = []
        calendar_error = None
        
        calendar_disabled, disabled_reason = is_calendar_disabled()
        if calendar_disabled:
            calendar_error = disabled_reason
        else:
            calendar_service = get_calendar_service()
            if calendar_service:
                try:
                    calendar_events = summarize_calendar_key_events(calendar_service, limit=limit)
                except Exception as calendar_exception:
                    disable_reason = should_disable_calendar_after_error(calendar_exception)
                    if disable_reason:
                        disable_calendar_temporarily(disable_reason)
                        calendar_error = (
                            f"{disable_reason}. Backing off calendar requests for "
                            f"{CALENDAR_RETRY_BACKOFF_MINUTES} minute(s)."
                        )
                    else:
                        calendar_error = str(calendar_exception)
                    print(f"Calendar summary warning: {calendar_error}")
        
        events = combine_key_events(calendar_events, sheet_events, limit=limit)
        counts = {
            'rehearsal': sum(1 for event in events if event['type'] == 'rehearsal'),
            'gig': sum(1 for event in events if event['type'] == 'gig')
        }
        
        return jsonify({
            'events': events,
            'counts': counts,
            'sources': {
                'calendar': len(calendar_events),
                'sheet': len(sheet_events)
            },
            'calendar_error': calendar_error,
            'calendar_disabled': bool(calendar_error and not calendar_events)
        })
    
    except Exception as e:
        print(f"ERROR in key_events: {str(e)}")
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