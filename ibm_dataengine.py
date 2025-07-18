import requests
import os


API_KEY = "Eez6r9f0hIfsN1oco46hjLeRgqpPY2HeWu-_O_6-rl7q"
DATA_ENGINE_CRN = "crn:v1:bluemix:public:sql-query:us-south:a/d8bbe8b298e445ad8511d740feefa195:02bbfdbe-958a-4336-b477-ad8533e8adda::"
access_token = ""


def get_access_token():
    global access_token
    return access_token


def get_refresh_token():
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic Yng6Yng=",
    }

    data = {"apikey": API_KEY, "grant_type": "urn:ibm:params:oauth:grant-type:apikey"}
    try:
        response = requests.post(
            "https://iam.cloud.ibm.com/identity/token", data=data, headers=headers
        )

        if response and response.status_code in range(200, 400):
            # Successful request
            token_data = response.json()
            global access_token
            access_token = token_data.get("access_token")
            print(f"Access Token: {access_token}")
        else:
            # Print error details
            print(f"Error: {response.status_code}")
            print(response.text)

        return access_token
    except Exception as e:
        print(f"An error occurred while getting access token: {str(e)}")
        exit()


def run_sql(sql_file_name, sql_query_params: "dict[str, str]" = dict()):
    access_token = get_access_token()

    # print(f"Access Token: {access_token}")

    # Specify the path to your file
    file_path = os.path.join(os.getcwd(), "sql", sql_file_name)

    try:
        # Open the file in read mode
        with open(file_path, "r") as file:
            # Read the content of the file
            file_content = file.read()

            for key, value in sql_query_params.items():
                file_content = file_content.replace(key, value)

            print(file_content)
        url = f"https://api.dataengine.cloud.ibm.com/v3/sql_jobs?instance_crn={DATA_ENGINE_CRN}"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        data = {"statement": file_content}

        response = requests.post(url, headers=headers, json=data)

        if response.status_code in range(200, 400):
            # Successfully submitted the SQL job, you can now work with the response content
            job_info = response.json()
            print(job_info)
            return job_info
        else:
            # Print an error message or handle the error as needed
            print(f"Error: {response.status_code} - {response.text}")
            if response.status_code in (401, 403):
                get_refresh_token()
            return None

    except FileNotFoundError:
        print(f"The file at path '{file_path}' does not exist")
        exit()
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        exit()


def get_jobid_status(jobid):
    """
    Get the current status of SQL Query with respective job-id in Data Engine
    """

    access_token = get_access_token()

    url = f"https://api.dataengine.cloud.ibm.com/v3/sql_jobs/{jobid}?instance_crn={DATA_ENGINE_CRN}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    response = requests.get(url, headers=headers)

    if response.status_code in range(200, 400):
        # Successfully received data, you can now work with the response content
        data: "dict[str]" = response.json()
        print(data["status"])
        if data["status"] == "failed":
            print("Error Message:", data["error_message"])
        return data["status"]
    else:
        # Print an error message or handle the error as needed
        print(f"Error: {response.status_code} - {response.text}")