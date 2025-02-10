import boto3
import uvicorn
import csv
import requests
from fastapi import FastAPI, UploadFile, File

# AWS Configuration
AWS_REGION = "us-east-1"  
ASU_ID = "1229592925"  
S3_BUCKET = f"{ASU_ID}-in-bucket"
SIMPLEDB_DOMAIN = f"{ASU_ID}-simpleDB"
CSV_URL = "https://raw.githubusercontent.com/CSE546-Cloud-Computing/CSE546-SPRING-2025/datasets/Classification%20Results%20on%20Face%20Dataset%20(1000%20images).csv"



s3_client = boto3.client("s3")
sdb_client = boto3.client("sdb")


def create_s3_bucket():
    try:
        s3_client.create_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' created successfully in region '{AWS_REGION}'.")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{S3_BUCKET}' already exists and is owned by you.")
        pass
    except Exception as e:
        print(f"An error occurred while creating bucket: {e}")


def create_simpledb_domain():
    try:
        response = sdb_client.list_domains()
        existing_domains = response.get("DomainNames", [])

        if SIMPLEDB_DOMAIN in existing_domains:
            print(f"SimpleDB domain '{SIMPLEDB_DOMAIN}' already exists.")
        else:
            sdb_client.create_domain(DomainName=SIMPLEDB_DOMAIN)
            print(f"SimpleDB domain '{SIMPLEDB_DOMAIN}' created successfully.")
            populate_simpledb()

    except Exception as e:
        print(f"An error occurred while creating the domain in simple DB: {e}")


def populate_simpledb():
    try:
        response = requests.get(CSV_URL)
        response.raise_for_status()  
        
        csv_data = response.text.splitlines()
        reader = csv.reader(csv_data)

        next(reader)
        print("Inserting Classification Data into SimpleDB Domain...")
        for row in reader:
            if len(row) != 2:
                continue
            
            image_name, classification_result = row[0], row[1]

            sdb_client.put_attributes(
                DomainName=SIMPLEDB_DOMAIN,
                ItemName=image_name,
                Attributes=[{"Name": "Result", "Value": classification_result, "Replace": True}],
            )
        print("Classification Data successfully inserted into SimpleDB Domain.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching CSV file: {e}")
    except Exception as e:
        print(f"An error occurred while inserting data in simple DB Domain: {e}")

create_s3_bucket()
create_simpledb_domain()


app = FastAPI()

@app.post("/")
async def upload_image(inputFile: UploadFile = File(...)):
    filename = inputFile.filename.split(".")[0] 
    
    s3_client.upload_fileobj(inputFile.file, S3_BUCKET, inputFile.filename)
    
    response = sdb_client.get_attributes(DomainName=SIMPLEDB_DOMAIN, ItemName=filename, AttributeNames=["Result"])
    print("get response from sdb: ", response)
    if "Attributes" in response:
        result = response["Attributes"][0]["Value"]
    else:
        result = "Unknown"
    
    return f"{filename}:{result}"

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
