# Python Medalists Project

## Setup Instructions

1. Open the Command Prompt and run the following commands:
    - git clone https://github.com/Hakdoff/python_medalists.git
    - cd python_medalists
    - code .

2. Open the VS Code terminal and create a virtual environment:
    - python -m venv venv

3. Activate the virtual environment:
- On Windows:
  ```
  venv\Scripts\Activate
  ```

4. Install the required dependencies:
    - pip install -r api/requirements.txt
    - pip install -r service/requirements.txt

5. Navigate to the `api` directory and Run the API server:
    - uvicorn main:app --reload

6. Open a new terminal, activate the venv and run the background_service:
    - python service/background_service.py

7. Open `http://127.0.0.1:8000/docs` in your default browser.

8. Under the **POST** section, upload the `medalists.csv` file.

9. Ensure both the server and background service are running.

10. If the response indicates the CSV was successfully added, wait for the background service to finish processing the CSV and archiving it.

11. Once the CSV file is archived, go to `http://127.0.0.1:8000/docs`, click the **GET** section, then click "Try it out" and "Execute" to fetch the data from the database.

12. Ensure that MongoDB is running for proper data storage and fetching.

