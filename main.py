import json
import pandas as pd
import uuid
from difflib import SequenceMatcher

#Loading raw_data.xlsx and parsing json inside the raw_data's raw_content
def load_parse_data(file_path: str):
    try:
        raw_data = pd.read_excel(file_path)
        raw_data["parsed_content"] = raw_data["raw_content"].apply(json.loads)
        parsed_data = pd.json_normalize(raw_data["parsed_content"])
        return raw_data, parsed_data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        exit()
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        exit()


#creating dim_comm_type sheet for final_data.xlsx table
def create_dim_comm_type(raw_data):
    dim_comm_type = raw_data[["comm_type"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_comm_type["comm_type_id"] = range(1, len(dim_comm_type)+1)
    print("dim_comm_type:")
    print(dim_comm_type)
    return dim_comm_type

#creating dim_subject sheet for final_data.xlsx table
def create_dim_subject(raw_data: pd.DataFrame):
    dim_subject = raw_data[["subject"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_subject["subject_id"] = range(1, len(dim_subject)+1)
    print("dim_subject:")
    print(dim_subject)
    return dim_subject

#creating dim_calendar sheet for final_data.xlsx table
def create_dim_calendar(parsed_data: pd.DataFrame) -> pd.DataFrame:
    dim_calendar = parsed_data[["calendar_id"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_calendar.rename(columns={"calendar_id": "raw_calendar_id"}, inplace=True)
    dim_calendar["calendar_id"] = range(1, len(dim_calendar) + 1)
    print("dim_calendar:")
    print(dim_calendar)
    return dim_calendar

#creating dim_datetime sheet for final_data.xlsx table
def create_dim_datetime(parsed_data: pd.DataFrame):
    dim_datetime = parsed_data[["dateString"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_datetime["datetime_id"] = range(1, len(dim_datetime)+1)
    dim_datetime["date"] = pd.to_datetime(dim_datetime["dateString"], utc=True)
    dim_datetime["date"] = dim_datetime["date"].dt.tz_localize(None)
    dim_datetime["year"] = dim_datetime["date"].dt.year
    dim_datetime["month"] = dim_datetime["date"].dt.month
    dim_datetime["day"] = dim_datetime["date"].dt.day
    dim_datetime["hour"] = dim_datetime["date"].dt.hour
    dim_datetime["minute"] = dim_datetime["date"].dt.minute
    dim_datetime = dim_datetime[['datetime_id', 'dateString', 'date', 'year', 'month', 'day', 'hour', 'minute']]
    print("dim_datetime:")
    print(dim_datetime)
    return dim_datetime

#creating dim_audio sheet for final_data.xlsx table
def create_dim_audio(parsed_data: pd.DataFrame):
    dim_audio = parsed_data[["audio_url"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_audio["audio_id"] = range(1, len(dim_audio)+1)
    dim_audio.rename(columns={"audio_url":"raw_audio_url"}, inplace=True)
    print("dim_audio:")
    print(dim_audio)
    return dim_audio

#creating dim_video sheet for final_data.xlsx table
def create_dim_video(parsed_data: pd.DataFrame):
    dim_video = parsed_data[["video_url"]].dropna().drop_duplicates().reset_index(drop=True)
    if dim_video.empty:
        dim_video = pd.DataFrame({'raw_video_url': [None], 'video_id': [None]})
    else:
        dim_video['video_id'] = range(1, len(dim_video) + 1)
        dim_video.rename(columns={'video_url': 'raw_video_url'}, inplace=True)
    print("dim_video:")
    print(dim_video)
    return dim_video

#creating dim_transcript sheet for final_data.xlsx table
def create_dim_transcript(parsed_data: pd.DataFrame):
    dim_transcript = parsed_data[["transcript_url"]].dropna().drop_duplicates().reset_index(drop=True)
    dim_transcript["transcript_id"] = range(1, len(dim_transcript)+1)
    dim_transcript.rename(columns={"transcript_url":"raw_transcript_url"}, inplace=True)
    print("dim_transcript:")
    print(dim_transcript)
    return dim_transcript

#creating dim_user sheet for final_data.xlsx table
def create_dim_user(raw_data: pd.DataFrame, parsed_data: pd.DataFrame):
    user_list = [] #list of dictionaries
    email_set = set()
    name_to_email = {}
    email_to_name = {}

    #collecting email
    for index, row in raw_data.iterrows():
        parsed = row["parsed_content"]
        if parsed["host_email"] and pd.notna(parsed["host_email"]):
            email_set.add(parsed["host_email"])
        if parsed["organizer_email"] and pd.notna(parsed["organizer_email"]):
            email_set.add(parsed["organizer_email"])
        for participant in parsed["participants"]:
            if participant and pd.notna(participant):
                email_set.add(participant)
        for attendee in parsed["meeting_attendees"]:
            if attendee["email"] and pd.notna(attendee["email"]):
                email_set.add(attendee["email"])
                if attendee["name"] and pd.notna(attendee["name"]):
                    name_to_email[attendee["name"]] = attendee["email"]
                    email_to_name[attendee["email"]] = attendee["name"]

    #processing the table
    for index, row in raw_data.iterrows():
        parsed = row["parsed_content"]
        #add host_emails data
        if parsed['host_email'] and pd.notna(parsed['host_email']):
            user_list.append({
                "name": email_to_name.get(parsed["host_email"], None),
                "email":parsed["host_email"],
                "location":None,
                "displayName":None,
                "phoneNumber":None
                })
            
        #add organizer_email data
        if parsed['organizer_email'] and pd.notna(parsed['organizer_email']):
            user_list.append({
                "name": email_to_name.get(parsed["organizer_email"], None),
                "email":parsed["organizer_email"], 
                "location":None, 
                "displayName":None, 
                "phoneNumber":None
                })
            
        #add participants data
        for participant in parsed["participants"]:
            if participant and pd.notna(participant):
                user_list.append({
                    "name":email_to_name.get(participant, None),
                    "email":participant,
                    "location":None,
                    "displayName":None,
                    "phoneNumber":None
                    })
                
        #add meeting_attendees data
        for attendee in parsed["meeting_attendees"]:
            if attendee["email"] and pd.notna(attendee["email"]):
                user_list.append({
                    "name": attendee["name"] if attendee["name"] and pd.notna(attendee["name"]) else email_to_name.get(attendee["email"], None),
                    "email": attendee["email"],
                    "location": attendee["location"],
                    "displayName": attendee["displayName"],
                    "phoneNumber": attendee["phoneNumber"]
                })

        #add speakers data
        for speaker in parsed["speakers"]:
            if speaker["name"] and pd.notna(speaker["name"]):
                apt_email = name_to_email.get(speaker["name"], None)
                if not apt_email:
                    name_lower = speaker["name"].lower()
                    name_parts = speaker["name"].split()
                    best_match_score = 0
                    best_match_email = None
                    for email in email_set:
                        if email and pd.notna(email):
                            email_lower = email.lower()
                            email_local = email_lower.split('@')[0]  
                            if len(name_parts) >= 2:
                                initials = name_parts[0][0].lower() + name_parts[1][0].lower()
                                if initials in email_local and email_local.endswith(name_parts[1].lower()):
                                    apt_email = email
                                    break
                            # Fuzzy matching
                            score = SequenceMatcher(None, name_lower, email_local).ratio()
                            if score > best_match_score and score > 0.7:  
                                best_match_score = score
                                best_match_email = email
                    if not apt_email and best_match_email:
                        apt_email = best_match_email
                    if not apt_email and email_set:  
                        apt_email = next(iter(email_set))
                user_list.append({
                    "name": speaker["name"],
                    "email" : apt_email,
                    "location": None,
                    "displayName": None,
                    "phoneNumber": None
                })

    dim_user = pd.DataFrame(user_list)
    dim_user = dim_user.drop_duplicates(subset=["email", "name"]).reset_index(drop=True)
    dim_user["user_id"] = [str(uuid.uuid4()) for _ in range(len(dim_user))]
    dim_user = dim_user[['user_id', 'email', 'name', 'location', 'displayName', 'phoneNumber']]
    print("dim_user:")
    print(dim_user)
    print(f"Null emails in dim_user: {dim_user['email'].isnull().sum()}")
    print(f"Null names in dim_user: {dim_user['name'].isnull().sum()}")
    return dim_user
    
#creating fact_communication sheet for final_data.xlsx table
def create_fact_communication(raw_data: pd.DataFrame, parsed_data: pd.DataFrame, dim_comm_type: pd.DataFrame, dim_subject: pd.DataFrame, dim_calendar: pd.DataFrame, dim_datetime: pd.DataFrame, dim_audio: pd.DataFrame, dim_transcript: pd.DataFrame, dim_video: pd.DataFrame):
    fact_communication = raw_data[["id", "source_id", "comm_type", "ingested_at", "processed_at", "is_processed", "subject"]].copy()
    fact_communication.rename(columns={'id': 'comm_id'}, inplace=True)

    fact_communication["raw_id"] = parsed_data["id"]
    fact_communication["datetime_id"] = parsed_data["dateString"]
    fact_communication["raw_title"] = parsed_data["title"]
    fact_communication["raw_duration"] = parsed_data["duration"]
    fact_communication["raw_calendar_id"] = parsed_data["calendar_id"]
    fact_communication["raw_audio_url"] = parsed_data["audio_url"]
    fact_communication["raw_video_url"] = parsed_data["video_url"]
    fact_communication["raw_transcript_url"] = parsed_data["transcript_url"]

    fact_communication = fact_communication.merge(dim_comm_type, on="comm_type", how="left")
    fact_communication = fact_communication.merge(dim_subject, on="subject", how="left")
    fact_communication = fact_communication.merge(dim_calendar, left_on="raw_calendar_id", right_on="raw_calendar_id", how="left")
    fact_communication = fact_communication.merge(dim_audio, on="raw_audio_url", how="left")
    fact_communication = fact_communication.merge(dim_video, on="raw_video_url", how="left")
    fact_communication = fact_communication.merge(dim_transcript, on="raw_transcript_url", how="left")

    fact_communication = fact_communication[[
        'comm_id', 'raw_id', 'source_id', 'comm_type_id', 'subject_id', 'calendar_id', 
        'audio_id', 'video_id', 'transcript_id', 'datetime_id', 'ingested_at', 
        'processed_at', 'is_processed', 'raw_title', 'raw_duration'
    ]]
    print("fact_communication:")
    print(fact_communication)
    print(f"Null foreign keys in fact_communication:")
    print(fact_communication[['comm_type_id', 'subject_id', 'calendar_id', 'datetime_id', 'audio_id', 'transcript_id', 'video_id']].isnull().sum())
    return fact_communication

#creating bridge_comm_user sheet for final_data.xlsx table
def create_bridge_comm_user(raw_data: pd.DataFrame, parsed_data: pd.DataFrame, dim_user: pd.DataFrame):
    bridge_list = []
    for index, row in raw_data.iterrows():
        comm_id = row["id"]
        parsed = row["parsed_content"]
        #organizer
        if parsed["host_email"] and pd.notna(parsed["host_email"]):
            bridge_list.append({
                "comm_id" : comm_id,
                "email" : parsed["host_email"],
                "isAttendee" : False,
                "isParticipant" : False,
                "isSpeaker": False,
                "isOrganiser" : True
            })
        if parsed['organizer_email'] and pd.notna(parsed['organizer_email']):
            bridge_list.append({
                "comm_id" : comm_id,
                "email" : parsed["organizer_email"],
                "isAttendee" : False,
                "isParticipant" : False,
                "isSpeaker": False,
                "isOrganiser" : True
            })

        #participant
        for participant in parsed["participants"]:
            if participant and pd.notna(participant):
                bridge_list.append({
                    "comm_id" : comm_id,
                    "email" : participant,
                    "isAttendee" : False,
                    "isParticipant" : True,
                    "isSpeaker": False,
                    "isOrganiser" : False
                })
        #attendee
        for attendee in parsed["meeting_attendees"]:
            if attendee["email"] and pd.notna(attendee["email"]):
                bridge_list.append({
                    "comm_id" : comm_id,
                    "email" : attendee["email"],
                    "isAttendee" : True,
                    "isParticipant" : False,
                    "isSpeaker": False,
                    "isOrganiser" : False
                })

        #speakers
        for speaker in parsed["speakers"]:
            if speaker["name"] and pd.notna(speaker["name"]):
                apt_email = next(
                    (email for name, email in dim_user.set_index("name")["email"].items() if name == speaker["name"]), None
                )
                bridge_list.append({
                    "comm_id" : comm_id,
                    "email" : apt_email,
                    "isAttendee" : False,
                    "isParticipant" : False,
                    "isSpeaker": True,
                    "isOrganiser" : False
                })

    bridge_df = pd.DataFrame(bridge_list)
    bridge_df = bridge_df.merge(dim_user[["user_id", "email"]], on="email", how="left")

    print("Null user_id count after email merge:", bridge_df['user_id'].isnull().sum())
    
    if bridge_df['user_id'].isnull().sum() > 0:
        bridge_df = bridge_df.merge(dim_user[["user_id", "name"]], on="name", how="left", suffixes=('_email', '_name'))
        bridge_df['user_id'] = bridge_df['user_id_email'].fillna(bridge_df['user_id_name'])
        bridge_df = bridge_df.drop(columns=['user_id_email', 'user_id_name'])
        if 'name' in bridge_df.columns:
            bridge_df = bridge_df.drop(columns=['name'])
    else:
        if 'name' in bridge_df.columns:
            bridge_df = bridge_df.drop(columns=['name'])
    available_columns = [col for col in ['comm_id', 'user_id', 'isAttendee', 'isParticipant', 'isSpeaker', 'isOrganiser'] if col in bridge_df.columns]
    bridge_df = bridge_df[available_columns]
    print("bridge_comm_user:")
    print(bridge_df)
    print(f"Null user_id in bridge_comm_user: {bridge_df['user_id'].isnull().sum()}")
    return bridge_df

def main():
    #load data
    raw_data, parsed_data = load_parse_data("source/raw_data.xlsx")

    #creating tables

    dim_comm_type = create_dim_comm_type(raw_data)
    dim_subject = create_dim_subject(raw_data)
    dim_calendar = create_dim_calendar(parsed_data)
    dim_datetime = create_dim_datetime(parsed_data)
    dim_audio = create_dim_audio(parsed_data)
    dim_transcript = create_dim_transcript(parsed_data)
    dim_video = create_dim_video(parsed_data)
    dim_user = create_dim_user(raw_data, parsed_data)
    fact_communication = create_fact_communication(raw_data, parsed_data, dim_comm_type, dim_subject, dim_calendar, dim_datetime, dim_audio, dim_transcript, dim_video)
    bridge_comm_user = create_bridge_comm_user(raw_data, parsed_data, dim_user)

    #export to xlsx
    with pd.ExcelWriter("output/final_data.xlsx") as writer:
        fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
        dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
        dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
        dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
        dim_datetime.to_excel(writer, sheet_name="dim_datetime", index=False)
        dim_user.to_excel(writer, sheet_name="dim_user", index=False)
        dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
        dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
        dim_video.to_excel(writer, sheet_name="dim_video", index=False)
        bridge_comm_user.to_excel(writer, sheet_name="bridge_comm_user", index=False)

if __name__=="__main__":
    main()