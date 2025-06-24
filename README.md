# ClimaStation

**ClimaStation** is a backend platform for parsing, analyzing, and visualizing climate data — starting with datasets from the Deutscher Wetterdienst (DWD), and designed to be easily extendable to other sources in the future.

## Features

- 🔄 Modular architecture with FastAPI
- 🌍 Support for DWD weather and climate data
- 🧱 Extensible feature system for additional providers
- 🧪 Built-in testing setup
- ⚙️ Environment-based configuration with `.env`


## Project Structure

CLIMASTATION-BACKEND/  
├── app/  
│ ├── core/ # Configuration and database setup  
│ ├── features/ # Modular features (e.g. DWD)  
│ └── main.py # FastAPI entry point  
├── tests/ # Unit and integration tests  
├── .vscode/ # VS Code project settings  
├── .env # Environment variables (excluded from Git)  
├── .gitignore # Ignored files and folders  
├── README.md # Project documentation  
└── requirements.txt # Python dependencies  


## Getting Started

1. Clone the repo:   
   git clone https://github.com/thiagobarrosjp/ClimaStation.git
   cd ClimaStation
   
2. Create and activate a virtual environment:  
   python -m venv venv  
   .\venv\Scripts\activate       # On Windows  
   source venv/bin/activate     # On macOS/Linux  

3. Install dependencies:  
   pip install -r requirements.txt

4. Run FASTAPI app:  
   uvicorn app.main:app --reload
