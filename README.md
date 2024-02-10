# YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

**Project Title:** YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

**Introduction:**
YouTube has become a significant platform for content creators and marketers alike. Understanding the dynamics of YouTube channels and videos can provide valuable insights for various purposes, including audience engagement, content strategy, and marketing campaigns. This project aims to create a comprehensive tool using Python, MySQL, MongoDB, and Streamlit to harvest data from multiple YouTube channels, store it in a data lake, migrate it to a SQL data warehouse, and provide users with interactive analysis capabilities through a Streamlit application.

**Skills Takeaway From This Project:**
- Python scripting for data collection and application development
- Utilization of MongoDB for storing unstructured and semi-structured data in a data lake
- Streamlit for building interactive web applications with ease
- Integration of APIs, specifically the YouTube API, for data retrieval
- Data management techniques using MongoDB (Compass) and MySQL Databases
- Querying SQL databases for data retrieval and analysis

**Domain:** Social Media

**Problem Statement:**
The task is to develop a Streamlit application allowing users to access and analyze data from multiple YouTube channels. Key features include:
- Retrieving relevant data (e.g., channel name, subscribers, video count, likes, dislikes, comments) using the YouTube API.
- Storing collected data in a MongoDB database as a data lake.
- Collecting data for up to 10 different YouTube channels and storing them in the data lake.
- Migrating data from the data lake to a SQL database for further analysis.
- Providing search and retrieval functionalities from the SQL database, including joining tables for comprehensive channel details.

**Approach:**
1. **Set up a Streamlit app:** Create a user-friendly interface for data input and visualization using Streamlit.
2. **Connect to the YouTube API:** Utilize the Google API client library to retrieve channel and video data.
3. **Store data in a MongoDB data lake:** Store collected data in MongoDB, suitable for handling unstructured data.
4. **Migrate data to a SQL data warehouse:** Transfer data from MongoDB to a SQL database (MySQL) for structured storage and analysis.
5. **Query the SQL data warehouse:** Employ SQL queries, potentially with libraries to fetch and manipulate data.
6. **Display data in the Streamlit app:** Present the retrieved data through interactive visualizations and insights using Streamlit's capabilities.

**Documentation Reference Links:**
- Streamlit Documentation: https://docs.streamlit.io/
- MySQL Documentation : https://dev.mysql.com/doc/
- MySQL connector Documentation : https://dev.mysql.com/doc/connector-python/en/
- MongoDB Documentation: https://docs.mongodb.com/
- pymongo Documentation : https://pymongo.readthedocs.io/en/stable/tutorial.html
- Google API Client Library for Python: https://developers.google.com/api-client-library/python
- YouTube API : https://developers.google.com/youtube/v3/docs


**Conclusion:**
This project offers a comprehensive solution for accessing and analyzing YouTube data, empowering users to gain insights into channel performance and audience engagement. By leveraging Python, SQL, MongoDB, and Streamlit, it provides a powerful toolkit for social media analysts, content creators, and marketers to enhance their strategies and decision-making processes.
