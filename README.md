This project revolves around the development of a robust data management and analysis system for a hypermarket chain. It involves the creation of a sophisticated data warehouse infrastructure, specifically designed to handle and analyze sales data from two primary channels: online and offline.

Technical Details:

Data Integration and ETL: The project begins with the extraction, transformation, and loading (ETL) process. We gather sales data from CSV files, integrate it into a PostgreSQL database, and conduct essential data transformations. This step ensures that the raw data is ready for storage and analysis.

3NF Data Modeling: We employ a 3rd Normal Form (3NF) data modeling approach to structure the data effectively. This process involves defining dimensions and fact tables, establishing relationships, and ensuring data integrity.

Dimensional Modeling: In parallel, we implement a dimensional model, optimizing the database for analytical queries. Dimension tables are created for attributes like stores, items, ordering methods, payment methods, customer types, and order priorities. These dimensions enable users to drill down into data for in-depth analysis.
