# Project Title
Info535 Project Pipeline

## Description
This project is a data pipeline that downloads, transforms, and analyzes data, with results visualized using Python and MongoDB.

## Features
- Downloads and processes data from a remote source.
- Stores raw and transformed data in MongoDB.
- Runs analyses and generates visualizations.
- Restarts the pipeline from the last checkpoint or from scratch.

## How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/akshaykishanck/INFOI535-FinalProject.git
2. Go to the project
   ```bash
   cd INFOI535-FinalProject
3. Setup MongoDB and start service:
   ```bash
   ./mongo_setup.sh
4. Run the pipeline:
   ```bash 
   python main.py --reset_and_start
