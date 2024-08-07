import csv
import os

import firebase_admin
from firebase_admin import credentials, firestore

# Initialize the Firebase Admin SDK
cred = credentials.Certificate('C:/Users/aayus/Downloads/fire/firekey.json')
firebase_admin.initialize_app(cred)

# Initialize Firestore
db = firestore.client()

def save_subcollection_data(user_id, subcollection_name, subcollection_data):
    """Save subcollection data to a separate CSV file."""
    csv_filename = f'C:/Users/aayus/Downloads/{subcollection_name}.csv'
    fieldnames = set()

    for item in subcollection_data:
        fieldnames.update(item.keys())

    fieldnames = sorted(fieldnames)

    file_exists = os.path.isfile(csv_filename)
    
    with open(csv_filename, mode='a', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=['user_id'] + fieldnames)
        if not file_exists:
            writer.writeheader()
        
        for item in subcollection_data:
            item['user_id'] = user_id  # Add user_id to subcollection data
            writer.writerow(item)

def get_document_data(doc_ref, save_subcollections=True):
    doc = doc_ref.get()
    if not doc.exists:
        return None
    
    doc_data = doc.to_dict()
    doc_data['id'] = doc.id

    # Fetch subcollections
    if save_subcollections:
        subcollections = doc_ref.collections()
        for subcollection in subcollections:
            sub_docs = subcollection.stream()
            subcollection_data = []
            for sub_doc in sub_docs:
                sub_doc_data = sub_doc.to_dict()
                sub_doc_data['id'] = sub_doc.id
                subcollection_data.append(sub_doc_data)
            if subcollection_data:
                save_subcollection_data(doc.id, subcollection.id, subcollection_data)

    return doc_data

def get_data_from_firestore(collection_name, csv_filename):
    try:
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()

        all_data = []
        fieldnames = set()

        # Process documents to collect all fields and data
        for doc in docs:
            doc_data = get_document_data(doc.reference)
            if doc_data:
                all_data.append(doc_data)
                fieldnames.update(doc_data.keys())

        fieldnames = sorted(fieldnames)  # Sort field names for consistent order

        # Write main collection data to CSV file
        with open(csv_filename, mode='w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['id'] + fieldnames)
            writer.writeheader()
            for data in all_data:
                writer.writerow(data)

        print(f'Data saved to {csv_filename}')
    except Exception as e:
        print(f'An error occurred: {e}')

# Example usage
get_data_from_firestore('users', 'C:/Users/aayus/Downloads/users_data_main.csv')
