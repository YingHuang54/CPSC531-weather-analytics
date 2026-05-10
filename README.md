# WeatherBig Data Analytics Application: OLAP on Weather Data
Course project for:
- School: California State University, Fullerton
- Course: CPSC 531 Advanced Database System
- Professor: Tseng-Ching James Shen
- Semester: Spring 2026


Implements an OLAP-style web application for interactive visualization of the global weather data. 

### Objective 
Gain hands-on practice in big data processing using Spark and managing data in HDFS.


## Technologies Used

### Data Processing
- Apache Spark
- Reverse Geocoder

### Backend
- Flask
- Flask-Cors
- Pandas
- Plotly Express

### Frontend
- Node.js
- React
- Plotly.js

## Dataset
- Weather-5K dataset https://github.com/taohan10200/WEATHER-5K/tree/main

## Data Preparation and Backend Configuration
1.	Set up HDFS (optional).
2.	Set up Spark
3.	Download and unzip the Weather-5K dataset into Spark’s configured filesystem (HDFS or local).
4.	Change the backend configuration file (backend/config.py) according to your environment.
5.	Install Python 3.12.
6.	(Recommended) Move inside the backend folder, create and activate a virtual environment:
```
python -m venv venv
.\venv\Scripts\activate
```
7.	install all necessary packages using the commands:
```pip install -r requirements.txt```
8.	Run to_parquet.py within the backend folder to convert the CSV files of the Weather-5K dataset to parquet format.
```python -m to_parquet```
9.	Run preprocess.py to generate the precomputed tables and Location Map JSON.
```python -m preprocess```
10.	Run the backend Flask App once from the backend folder and note down its URL address in console.
```python -m flask_app.app```

## Frontend Setup Instruction
1.	Install Node.js.
2.	Run the command within the frontend folder to install all required packages:
```npm ci```  or ```npm install```
3.	(Optional) Run the frontend React App once to get its URL address within the frontend folder using the command:
```npm run dev```
4.	Set the value of FRONTEND_URL in backend/config.py to the Frontend React App URL.
5.	If different from the default in frontend/src/config.json, change the value for “backendAddress” to the Backend Flask App URL Address. If it is unknown, see Section 5.2 step 8.

## Run Application
1.	Start HDFS if the Spark’s configured filesystem is HDFS.
2.	Move inside the backend folder and activate the virtual environment as needed:
```.\venv\Scripts\activate```
3.	Within the backend folder, run the command in the terminal: ```python -m flask_app.app```
4.	Open a separate terminal for frontend folder.
5.	Within the frontend folder, run the command in the terminal: 
```npm run dev```

