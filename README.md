# reconcilation

A Django, DRF backend application that performs some reconcilation functionality

## Prerequisites

- Python (> 3.10.x)

## Setup

1. Clone the repository into a folder of your choice:

   ```shell
   git clone https://github.com/jayjaychukwu/reconcilation.git
   ```

2. Enter the reconcilation directory and spin up a virtual environment for the project using Python:

    ```shell
    python -m pip install virtualenv
    python -m virtualenv venv
    ```

   This should create your virtual environment

   ```shell
   . venv/bin/activate
   ```

   OR

    ```shell
   source venv/bin/activate
   ```

3. Install the dependencies:

    ```shell
   pip install -r requirements.txt
   ```

4. Make migrations

   ```shell
   python manage.py makemigrations
   python manage.py migrate
   ```

5. Start the web server and the celery server
      - In one terminal

         ```shell
         python manage.py runserver  
         ```

      - In another terminal

         ```shell
         celery -A reconcilation worker -l INFO  
         ```

- To create a superuser for the admin dashboard, run the following command:
  
    ```shell
   python manage.py createsuperuser
    ```

    Follow the prompts to create a superuser account.
    Access the admin dashboard at `http://localhost:8000/admin/` and log in using your superuser credentials.

## API Documentation

- Swagger Docs: accessible at `http://localhost:8000`

## Architecture

The project follows a Django architecture and utilizes the Django REST Framework for building the API.
It utilizes Celery for asynchronous processing.
