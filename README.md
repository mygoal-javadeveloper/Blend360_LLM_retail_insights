# Designed & Developed By

## **Nisrin Dhoondia**


# Retail Insights Assistant, a LLM Multi-Agent System (Blend360 GenAI Assignment)

This project is a **GenAI-powered Retail Insights Assistant** that:
- Cleans large inconsistent retail CSV datasets
- Loads them into DuckDB
- Also creates master sales table
- Supports **Summaries** and **Conversational Q&A**
- Uses **Local Mistral (Ollama)** for SQL generation
- Provides a **Streamlit UI**

The repository contains a fully working **Retail Insights Assistant** built using:
- Local LLMs (Mistral via Ollama)
- Streamlit UI
- DuckDB as the analytics engine
- Multi-Agent architecture:
  - LanguageToSQLAgent (LLM generates SQL)
  - SQLExecutionAgent
  - ValidationAgent


## Features

- Auto-renames messy filenames
- Standardizes columns (lowercase, snake_case)
- Cleans values
- Loads all cleaned CSVs into DuckDB automatically
- Two unified structures created:
	– Each CSV table
	– `master_sales` table (merging compatible sales tables)

- Natural language to SQL using local Mistral model
- Fuzzy matching for dirty retail data
- Streamlit chatbot UI
  
  	**Modes:**
	- **Summarization Mode**       simple table summaries
	- **Conversational Q&A Mode**  LLM-generated SQL with result viewer
   
- DuckDB execution engine
- Multi-Agent Architecture
  
  	**Agents:**
	- **LanguageToSQLAgent**  converts natural language to SQL 
	- **ValidationAgent**     ensures only safe SELECT queries 
	- **SQLExecutionAgent**   executes SQL on DuckDB
   

## Folder Structure

Blend360_Retail_Insights/  

│ 

├── app.py 

├── utils.py 

├── agents.py 

├── requirements.txt 

├── README.md 

│ 

├── data/ 

│   ├── (All original CSV files given by HR) 
│   │ 
│   │  Use the Streamlit sidebar: 
│   │  - **Clean & (Re)Load All CSVs into DuckDB**  
│   │       - Cleans all CSVs  
│   │       - Each cleaned CSV becomes a DuckDB table 
│   │
│   │  - **Create master_sales…** 
│   │       - Creates a  `master_sales` DuckDB table merging compatible sales tables 

│ 

├── presentation/ 

│   ├── Architecture_Pipeline_PDF.pdf 

│   ├── Architecture_Pipeline_PPT.pptx 

│   ├── Streamlit_Demo.mp4 

│   ├── Sample_Questions.md 

│ 

└── .gitignore 


## Running the Project

#### 1. Install dependencies

```
pip install -r requirements.txt

```
#### 2. Install & run Mistral (Ollama)

```
ollama run mistral

```
#### 3. Start the Streamlit App

```
streamlit run app.py

```


## How to Test

#### Step 1 - Load Data

- Click on the sidebar controls
  
**Clean & (Re)Load all CSVs into DuckDB** 
**Create master_sales (merge compatible sales tables)**  

#### Step 2 - Summarization

- Select a table
- Click **Generate Summary**
- The LLM generates: 
  
   	– Column-level insights 
  	– Patterns and trends 
	– Anomalies 
	– Business interpretations 
	– Data quality issues 

- Output: 
  
    A structured, human-readable dataset summary
  
#### Step 3 - Conversational Q&A

- Examples you can type:
	1. What is the total amount of Amazon sales report?

	Table: All (Select All from dropdown list)

	2. Which are the states the product shipped?

	Table: amazon_sale_report (Select amazon_sale_report from dropdown list)

	3. How many null values in this table?

	Table: cloud_warehouse_compersion_chart (Select cloud_warehouse_compersion_chart from dropdown list)

	4. Which are the Increff in each Shiprocket?

	Table: cloud_warehouse_compersion_chart (Select cloud_warehouse_compersion_chart from dropdown list)

	5. What are the different expenses and its count?

	Table: expense_iigf (Select expense_iigf from dropdown list)

	6. What is the total gross amount for each customer?

	Table: international_sale_report (Select international_sale_report from dropdown list)

	7. What is the total quantity for each SKU?

	Table: master_sales (Select master_sales from dropdown list)

	8. What are the different categories and catalogs available?

	Table: may2022 (Select may2022 from dropdown list)

	9. Each catalog has how many categories?

	Table: may2022 (Select may2022 from dropdown list)

	10. What are the different catalogs available?

	Table: p__l_march_2021 (Select p__l_march_2021 from dropdown list)

	11. What are the different categories available?

	Table: sale_report (Select sale_report from dropdown list)

	12. What are the different sources?

	Table: unified_sales (Select unified_sales from dropdown list)
- The LLM will:
	- Generate SQL  
	- Validate SQL  
	- Execute SQL  
	- Display results as data table & JSON


## Limitations and Future Improvements

#### Limitations

- Fuzzy SQL rules may require tuning 
- LLM runs locally (Mistral via Ollama) 
- Deep summaries are slow on large data 

#### Possible Improvements

- Add important column auto-detection for faster summaries 
- Use metadata vector DB for column-search optimization 



