

import sys
from pydantic import BaseModel
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai import types
import os, random
import json
import time
import pandas as pd
from collections import deque
from datetime import datetime

class Review(BaseModel):
    positive_review: str
    negative_review: str
    society_management: str  # String ratings like "4" or "N.A."
    green_area: str
    amenities: str
    connectivity: str
    construction: str
    overall: str  # String like "3.8" or "N.A."
    duration_of_stay: str

class RateLimiter:
    def __init__(self, max_requests_per_minute=10, max_requests_per_day=200):
        self.max_rpm = max_requests_per_minute
        self.max_daily = max_requests_per_day
        self.request_times = deque()
        self.daily_count = 0
        self.last_day_check = datetime.now().day

    def check_limit(self):
        now = datetime.now()
        if now.day != self.last_day_check:
            self.daily_count = 0
            self.last_day_check = now.day
        
        if self.daily_count >= self.max_daily:
            raise Exception("Daily request limit reached")
        
        # Remove old requests outside the 1-minute window
        while self.request_times and (now - self.request_times[0]).total_seconds() > 60:
            self.request_times.popleft()
        
        if len(self.request_times) >= self.max_rpm:
            time_to_wait = 60 - (now - self.request_times[0]).total_seconds()
            if time_to_wait > 0:
                print(f"Rate limit reached. Waiting {time_to_wait:.2f} seconds...")
                time.sleep(time_to_wait)
        
        return True

    def record_request(self):
        now = datetime.now()
        self.request_times.append(now)
        self.daily_count += 1

class GeminiReviewGenerator:
    __model_name = 'gemini-2.0-flash'  # Updated to use a more stable model
    __prompt_file_path = 'gemini_ai_prompts.json'

    def __init__(self):
        load_dotenv()
        
        # Initialize API keys in round-robin fashion
        self.api_keys = self._load_api_keys()
        if not self.api_keys:
            raise ValueError("No API keys found for Gemini. Please set GEMINI_API_KEY or GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc. in the .env file.")
        
        self.current_key_index = 0
        self.rate_limiter = RateLimiter()
        # Store chats by project_name AND set_number combination
        self.project_chats = {}
        
        print(f"Initialized with {len(self.api_keys)} API key(s) for round-robin usage")

    def _load_api_keys(self):
        """Load API keys from environment variables"""
        api_keys = []
        
        # Try to load single API key first
        single_key = os.getenv("GEMINI_API_KEY")
        if single_key:
            api_keys.append(single_key)
        
        # Try to load multiple API keys (GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc.)
        key_index = 1
        while True:
            key = os.getenv(f"GEMINI_API_KEY_{key_index}")
            print(f"Checking GEMINI_API_KEY_{key_index}: {key}")
            if key:
                api_keys.append(key)
                key_index += 1
            else:
                break
        
        return api_keys

    def _get_next_api_key(self):
        """Get the next API key in round-robin fashion"""
        if not self.api_keys:
            raise ValueError("No API keys available")
        
        current_key = self.api_keys[self.current_key_index]
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        
        print(f"Using API key #{self.current_key_index if self.current_key_index == 0 else len(self.api_keys)} (Round-robin)")
        return current_key

    def _configure_api_with_key(self, api_key):
        """Configure the Gemini API with a specific key"""
        genai.configure(api_key=api_key)

    def __getPromptFromFile(self, type: str) -> str:
        try:
            with open(GeminiReviewGenerator.__prompt_file_path, 'r', encoding='utf-8') as file:
                data = file.read()
            parsed_data = json.loads(data)
            return parsed_data.get(type, "")
        except FileNotFoundError:
            print(f"Warning: Prompt file {GeminiReviewGenerator.__prompt_file_path} not found. Using default prompt.")
            return "Generate a detailed review based on the provided project information."
        except json.JSONDecodeError:
            print(f"Warning: Invalid JSON in {GeminiReviewGenerator.__prompt_file_path}. Using default prompt.")
            return "Generate a detailed review based on the provided project information."

    def _get_system_instruction_for_set(self, set_number):
        """
        Get the appropriate system instruction based on set number
        """
        # Define mapping of set numbers to system instruction keys
        instruction_mapping = {
            1: 'system_instruction_review_generator_resident',
            2: 'system_instruction_review_generator_family',
            3: 'system_instruction_review_generator_female',
            4: 'system_instruction_review_generator_old'
        }
        
        # Get the instruction key for the set number, default to resident if not found
        instruction_key = instruction_mapping.get(set_number, 'system_instruction_review_generator_resident')
        return self.__getPromptFromFile(instruction_key)

    def _initialize_chat_for_project_set(self, project_name, set_number):
        """
        Initialize a chat session for a specific project and set combination
        """
        # Get the next API key in round-robin fashion
        current_api_key = self._get_next_api_key()
        self._configure_api_with_key(current_api_key)
        
        # Get the appropriate system instruction for this set
        system_prompt = self._get_system_instruction_for_set(set_number)
        print(f"System prompt for set {set_number}: {system_prompt[:100]}...")
        
        # Create unique key for project-set combination
        chat_key = f"{project_name}_set_{set_number}"
        
        try:
            # Create a new model instance with system instruction
            model = genai.GenerativeModel(
                model_name=self.__model_name,
                system_instruction=system_prompt,
                generation_config=genai.GenerationConfig(
                    temperature=0.8,
                    top_p=0.7,
                    response_mime_type='application/json'
                )
            )
            
            # Start a chat session
            chat = model.start_chat(history=[])
            
            # Send initial context message
            initial_message = (
                f"I will be generating a review for the project '{project_name}' (Set {set_number}). "
                f"Please generate a review that matches the persona defined in the system instruction. "
                f"Response must be valid JSON matching the Review schema."
            )
            
            response = chat.send_message(initial_message)
            print(f"Initial response: {response.text[:100]}...")
            
            self.project_chats[chat_key] = chat
            print(f"✓ Initialized chat session for project: {project_name} - Set {set_number}")
            
        except Exception as e:
            print(f"Error initializing chat for {project_name} - Set {set_number}: {str(e)}")
            raise e

    def generate_review(self, project_info_df, project_name, set_number):
        print(f"Generating review for project '{project_name}' - Set {set_number}...")
        
        if not self.rate_limiter.check_limit():
            time.sleep(10)
            return self.generate_review(project_info_df, project_name, set_number)
        
        self.rate_limiter.record_request()
        time.sleep(random.uniform(0.5, 1.5))

        # Create unique key for project-set combination
        chat_key = f"{project_name}_set_{set_number}"
        
        # Initialize chat if it doesn't exist for this project-set combination
        if chat_key not in self.project_chats:
            self._initialize_chat_for_project_set(project_name, set_number)

        chat = self.project_chats.get(chat_key)
        if not chat:
            print(f"Failed to get chat for {chat_key}")
            return None

        # Get the next API key for this request
        current_api_key = self._get_next_api_key()
        self._configure_api_with_key(current_api_key)

        message_content = f"""
        Generate a detailed review for project '{project_name}' based on the following data:
        {project_info_df.to_json(orient='records')}
        
        This is Set {set_number}. Please ensure the review reflects the perspective and style 
        appropriate for this set while maintaining uniqueness.
        
        IMPORTANT: Return ONLY valid JSON with this exact structure:
        {{
            "positive_review": "Write detailed positive aspects here",
            "negative_review": "Write detailed negative aspects here", 
            "society_management": "4",
            "green_area": "3",
            "amenities": "4",
            "connectivity": "5",
            "construction": "4",
            "overall": "4.0",
            "duration_of_stay": "2 years"
        }}
        
        Rules:
        - positive_review and negative_review must be detailed text (not empty)
        - All rating fields must be strings: either "1", "2", "3", "4", "5" or "N.A."
        - overall should be calculated average as string (e.g. "3.8") or "N.A."
        - duration_of_stay should be from the data provided
        - Do not include any other fields like "review_text" or "ratings" object
        """

        try:
            response = chat.send_message(message_content)
            review_json = response.text
            print(f"Raw response: {review_json[:200]}...")
            
        except Exception as e:
            print(f'Gemini AI Chat execution threw an exception: {e}')
            try:
                # Re-initialize chat on error with different API key
                print("Attempting to re-initialize chat with different API key...")
                self._initialize_chat_for_project_set(project_name, set_number)
                chat = self.project_chats.get(chat_key)
                if chat:
                    # Use different API key for retry
                    retry_api_key = self._get_next_api_key()
                    self._configure_api_with_key(retry_api_key)
                    response = chat.send_message(message_content)
                    review_json = response.text
                else:
                    return None
            except Exception as e2:
                print(f"Re-initialization failed: {e2}")
                return None

        if not review_json:
            return None

        try:
            # Clean the JSON response (remove markdown formatting if present)
            review_json = review_json.strip()
            if review_json.startswith('```json'):
                review_json = review_json[7:]
            if review_json.endswith('```'):
                review_json = review_json[:-3]
            review_json = review_json.strip()
            
            review_data = json.loads(review_json)
            
            # Remove any unwanted nested structures
            if 'review_text' in review_data:
                del review_data['review_text']
            if 'ratings' in review_data:
                # If ratings are nested, extract them to top level
                ratings = review_data.pop('ratings')
                for key, value in ratings.items():
                    if key in ['society_management', 'green_area', 'amenities', 'connectivity', 'construction', 'overall']:
                        review_data[key] = value
                        
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Raw response: {review_json}")
            return None

        # Handle missing overall rating - calculate from other ratings
        if "overall" not in review_data or review_data["overall"] is None or review_data["overall"] == "":
            fields = ["society_management", "green_area", "amenities", "connectivity", "construction"]
            numeric_ratings = []
            for field in fields:
                val = review_data.get(field)
                if isinstance(val, (int, float)) and 1 <= val <= 5:
                    numeric_ratings.append(val)
                elif isinstance(val, str) and val.isdigit() and 1 <= int(val) <= 5:
                    numeric_ratings.append(int(val))
                elif isinstance(val, str) and val.replace('.', '').isdigit():
                    try:
                        num_val = float(val)
                        if 1 <= num_val <= 5:
                            numeric_ratings.append(num_val)
                    except ValueError:
                        pass
            
            if numeric_ratings:
                avg = sum(numeric_ratings) / len(numeric_ratings)
                review_data["overall"] = f"{avg:.1f}"
            else:
                review_data["overall"] = "N.A."

        # Convert all rating fields to strings and handle missing fields
        rating_fields = ["society_management", "green_area", "amenities", "connectivity", "construction", "overall"]
        for field in rating_fields:
            if field not in review_data or review_data[field] is None or review_data[field] == "":
                review_data[field] = "N.A."
            else:
                # Convert to string format
                val = review_data[field]
                if isinstance(val, (int, float)):
                    if field == "overall":
                        review_data[field] = f"{val:.1f}"
                    else:
                        review_data[field] = str(int(val))
                elif isinstance(val, str):
                    if val.lower() in ["na", "n.a.", "not available", "not applicable"]:
                        review_data[field] = "N.A."
                    else:
                        review_data[field] = str(val)

        # Handle duration of stay
        if "duration_of_stay" not in review_data or review_data["duration_of_stay"] is None or review_data["duration_of_stay"] == "":
            if 'duration_of_stay' in project_info_df.columns and not project_info_df['duration_of_stay'].empty:
                duration_val = project_info_df['duration_of_stay'].iloc[0]
                review_data["duration_of_stay"] = str(duration_val) if not pd.isna(duration_val) else "N.A."
            else:
                review_data["duration_of_stay"] = "N.A."

        # Ensure required fields exist and have proper values
        if "positive_review" not in review_data or not review_data["positive_review"]:
            review_data["positive_review"] = "No specific positive aspects mentioned."
        if "negative_review" not in review_data or not review_data["negative_review"]:
            review_data["negative_review"] = "No specific negative aspects mentioned."

        # Ensure only the required fields are present with proper string formatting
        final_data = {
            "positive_review": str(review_data.get("positive_review", "")),
            "negative_review": str(review_data.get("negative_review", "")),
            "society_management": str(review_data.get("society_management", "N.A.")),
            "green_area": str(review_data.get("green_area", "N.A.")),
            "amenities": str(review_data.get("amenities", "N.A.")),
            "connectivity": str(review_data.get("connectivity", "N.A.")),
            "construction": str(review_data.get("construction", "N.A.")),
            "overall": str(review_data.get("overall", "N.A.")),
            "duration_of_stay": str(review_data.get("duration_of_stay", "N.A."))
        }

        return json.dumps(final_data, ensure_ascii=False, indent=None, separators=(',', ':'))

    def get_chat_history(self, project_name, set_number):
        """
        Get chat history for a specific project and set combination
        """
        chat_key = f"{project_name}_set_{set_number}"
        chat = self.project_chats.get(chat_key)
        if not chat:
            return []
        
        try:
            history = chat.history
            return [
                {
                    'role': msg.role, 
                    'text': ''.join([part.text for part in msg.parts if hasattr(part, 'text')])
                }
                for msg in history
            ]
        except Exception as e:
            print(f"Error getting chat history: {e}")
            return []

def prepare_project_info_df(pname, set_phrases, duration, set_number):
    if not set_phrases or pd.isna(set_phrases) or set_phrases == "":
        return None
    
    phrases = [p.strip() for p in str(set_phrases).split('\n') if p.strip()]
    positive = [p.replace(" (positive)", "") for p in phrases if "(positive)" in p]
    negative = [p.replace(" (negative)", "") for p in phrases if "(negative)" in p]
    neutral = [p for p in phrases if "(positive)" not in p and "(negative)" not in p]
    
    return pd.DataFrame({
        'project_name': [pname],
        'positive_phrases': [positive],
        'negative_phrases': [negative],
        'neutral_phrases': [neutral],
        'duration_of_stay': [duration if not pd.isna(duration) else "NA"],
        'set_number': [set_number]
    })

def main():
    # Check if required files exist
    if not os.path.exists("output_sets.csv"):
        print("Error: output_sets.csv not found!")
        return
    
    if not os.path.exists("gemini_ai_prompts.json"):
        print("Warning: gemini_ai_prompts.json not found. Using default prompts.")
    
    try:
        df = pd.read_csv("output_sets.csv")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    if df.empty:
        print("Error: CSV file is empty!")
        return
    
    set_columns = [col for col in df.columns if col.startswith("Set ")]
    if not set_columns:
        print("Error: No 'Set' columns found in the CSV!")
        return
    
    output_file = "structured_reviews.csv"
    
    # Create output file if it doesn't exist
    if not os.path.exists(output_file):
        columns = ["xid", "Project name"] + [f"Review {i}" for i in range(1, len(set_columns)+1)]
        pd.DataFrame(columns=columns).to_csv(output_file, index=False)
        print(f"Created output file: {output_file}")
    
    try:
        gen = GeminiReviewGenerator()
    except Exception as e:
        print(f"Error initializing Gemini generator: {e}")
        return

    total_projects = len(df)
    successful_projects = 0
    
    for idx, (_, row) in enumerate(df.iterrows()):
        try:
            xid = row.get("xid", f"id_{idx}")
            pname = row.get("Project name", f"Project_{idx}")
            
            print(f"\n{'='*50}")
            print(f"Processing project {idx+1}/{total_projects}: {pname} (ID: {xid})")
            print(f"{'='*50}")
            
            pdata = {"xid": xid, "Project name": pname}
            project_success = True

            # Process each set with different system instructions
            for s in range(1, len(set_columns)+1):
                scol = f"Set {s}"
                dcol = f"How Long do you stay here {s}"
                
                print(f"\n--- Processing Set {s} ---")
                
                if scol not in row or pd.isna(row[scol]) or str(row[scol]).strip() == "":
                    print(f"Skipping Set {s}: No data available")
                    pdata[f"Review {s}"] = ""
                    continue
                
                pdf = prepare_project_info_df(pname, row[scol], row.get(dcol, "NA"), s)
                if pdf is None:
                    print(f"Skipping Set {s}: Could not prepare project info")
                    pdata[f"Review {s}"] = ""
                    continue
                    
                try:
                    print(f"Generating review for {pname} - Set {s}...")
                    rjson = gen.generate_review(pdf, pname, s)
                    
                    if rjson:
                        pdata[f"Review {s}"] = rjson
                        print(f"✓ Success: {pname} - Set {s}")
                    else:
                        raise Exception("No review generated")
                        
                except Exception as e:
                    print(f"✗ Failed for {pname} (Set {s}): {str(e)}")
                    project_success = False
                    
                    # Create error JSON
                    err_json = json.dumps({
                        "positive_review": f"Generation failed: {str(e)[:100]}", 
                        "negative_review": "",
                        "society_management": "N.A.", 
                        "green_area": "N.A.", 
                        "amenities": "N.A.",
                        "connectivity": "N.A.", 
                        "construction": "N.A.", 
                        "overall": "N.A.",
                        "duration_of_stay": "N.A."
                    })
                    pdata[f"Review {s}"] = err_json

            # Save data for this project
            try:
                # Convert the JSON string back to dict for proper CSV storage
                output_data = {}
                for key, value in pdata.items():
                    if key.startswith("Review ") and value:
                        try:
                            # Parse JSON and store as properly formatted JSON string
                            review_dict = json.loads(value)
                            output_data[key] = json.dumps(review_dict, ensure_ascii=False, separators=(',', ':'))
                        except:
                            output_data[key] = value
                    else:
                        output_data[key] = value
                
                pd.DataFrame([output_data]).to_csv(output_file, mode='a', header=False, index=False, quoting=1, escapechar=None)
                print(f"✓ Saved data for {pname} to {output_file}")
                if project_success:
                    successful_projects += 1
            except Exception as e:
                print(f"✗ Error saving data for {pname}: {e}")

        except Exception as e:
            print(f"✗ Critical error processing project {idx+1}: {e}")
            continue

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total projects processed: {total_projects}")
    print(f"Successful projects: {successful_projects}")
    print(f"Failed projects: {total_projects - successful_projects}")
    print(f"Output file: {output_file}")
    print(f"{'='*60}")

if __name__ == "__main__":

    main()
