import pandas as pd
import json
import numpy as np


df = pd.read_csv('gemini_structured_reviews.csv')


new_rows = []


for _, row in df.iterrows():
    xid = row['xid']
    project_name = row['Project name']
    
    # Loop through the Review columns
    for col in df.columns:
        if 'Review' in col and pd.notna(row[col]):
            try:
                # Parse the JSON in the review cell
                review_data = json.loads(row[col])
                
                # Get the positive and negative reviews
                positive = review_data.get('positive_review', 'N.A.')
                negative = review_data.get('negative_review', 'N.A.')
                
                # Get individual ratings
                society_management = review_data.get('society_management', 'N.A.')
                green_area = review_data.get('green_area', 'N.A.')
                amenities = review_data.get('amenities', 'N.A.')
                connectivity = review_data.get('connectivity', 'N.A.')
                construction = review_data.get('construction', 'N.A.')
                
                # Get the overall rating
                overall = review_data.get('overall', 'N.A.')
                
                # Add the new row to our list
                new_rows.append({
                    'xid': xid,
                    'project_name': project_name,
                    'positive': positive,
                    'negative': negative,
                    'society_management': society_management,
                    'green_area': green_area,
                    'amenities': amenities,
                    'connectivity': connectivity,
                    'construction': construction,
                    'overall_rating': overall
                })
            except (json.JSONDecodeError, AttributeError):
                # Skip if the review is not a valid JSON
                continue

# Create the new dataframe from our list of rows
new_df = pd.DataFrame(new_rows)

# Save to a new CSV file
new_df.to_csv('processed_reviews.csv', index=False)

print(f"Processed {len(new_rows)} reviews from {len(df)} projects.")
print("New CSV created as 'processed_reviews.csv'")