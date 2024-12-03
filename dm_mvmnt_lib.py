#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8
import pandas as pd

def edit_text_file(input_file, output_file):
    # Read the input file
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Delete rows 1, 2, 3, 4, and 6 (using 0-based index)
    rows_to_delete = [0, 1, 2, 3, 5]  # Convert 1-based to 0-based index
    new_lines = [line for i, line in enumerate(lines) if i not in rows_to_delete]

    # Delete the first character of every remaining row
    new_lines = [line[1:] if len(line) > 1 else line for line in new_lines]

    # Write the modified content to the output file
    with open(output_file, 'w') as file:
        file.writelines(new_lines)

# Define a custom date parser function
def custom_date_parser(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, '%m/%d/%Y')

def edit_text_file2(input_file, output_file, encoding='utf-8'):
    # Read the input file with the specified encoding
    with open(input_file, 'r', encoding=encoding) as file:
        lines = file.readlines()

    # Delete rows 1, 2, 3, 4 (using 0-based index)
    rows_to_delete = [0, 1, 2, 3]  # Convert 1-based to 0-based index
    new_lines = [line for i, line in enumerate(lines) if i not in rows_to_delete]

    # Delete the first character of every remaining row
    new_lines = [line[1:] if len(line) > 1 else line for line in new_lines]

    # Write the modified content to the output file with the specified encoding
    with open(output_file, 'w', encoding=encoding) as file:
        file.writelines(new_lines)

def dm_mvmnt_result(cutoff_date, dm_invent_csv, dm_movement_csv):

    from datetime import datetime

    cutoff_date = datetime.strptime(cutoff_date, '%m/%d/%Y')

    # Set the option to display all columns
    pd.set_option('display.max_columns', None)

    # Specify the input file and output file names
    input_file = dm_invent_csv  # Replace with your input file name
    output_file = 'dm_invent.csv'

    # Call the function to edit the text file
    edit_text_file(input_file, output_file)

    print(f"File has been edited and saved as {output_file}")

    # Import the CSV file with the custom date parser
    #df = pd.read_csv(output_file,  usecols=[1,8,18,27], parse_dates=['Shlf.Date'], date_parser=custom_date_parser)

    # Read the CSV file
    df = pd.read_csv(output_file, usecols=[1, 8, 18, 27], parse_dates=['Shlf.Date'], date_format='%m-%d-%y')

    # Set new column names using set_axis
    df = df.set_axis(['Material_1', 'Batch', 'Shlf_Date', 'TOTAL'], axis='columns')

    #df.info()

    
    from pandasql import sqldf

    # Define your SQL query
    query = """
    SELECT Material_1, Shlf_Date, Batch, sum(TOTAL) as TTL FROM df 
    GROUP BY Material_1,Shlf_Date, Batch
    ORDER BY Material_1,Shlf_Date, Batch
    """
    # Execute the query and create a new DataFrame
    df_invent = sqldf(query)
    
    # Display the new DataFrame
    print(df_invent)

    # Save DataFrame to CSV file
    df_invent.to_csv('dm_invent_short.csv', index=False)

    # Specify the input file and output file names
    input_file = dm_movement_csv  # Replace with your input file name
    output_file = 'dm_movement.csv'

    # Call the function to edit the text file
    edit_text_file2(input_file, output_file)

    print(f"File has been edited and saved as {output_file}")

    df2 = pd.read_csv(output_file, usecols=[2,9,13])

    # Remove blank records (rows with any NaN values)
    df2_cleaned = df2.dropna()

    # Save DataFrame to CSV file
    df2_cleaned.to_csv('dm_movement_2-9-13.csv', index=False)

    # Import the CSV file with the custom date parser
    #df2 = pd.read_csv('dm_movement_2-9-13.csv', parse_dates=['Posted on'], date_parser=custom_date_parser)
    
    # Read the CSV file
    df2 = pd.read_csv('dm_movement_2-9-13.csv', parse_dates=['Posted on'], date_format='%m-%d-%Y')

    # Save DataFrame to CSV file
    df2.to_csv('dm_movement_2-9-13.csv', index=False)

    # Set new column names using set_axis
    df2 = df2.set_axis(['Material', 'Posted_on', 'Qty_in_UnE'], axis='columns')

    df2['Posted_on'] = pd.to_datetime(df2['Posted_on'])
    df2.info()

    # Define your SQL query
    query = """
    SELECT Material, strftime('%Y-%m', Posted_on) AS month, SUM(Qty_in_UnE) AS total_value
    FROM df2
    GROUP BY Material,month
    ORDER BY Material,month
    """
    # Execute the query and create a new DataFrame
    # df_movement = sqldf(query, globals())
    df_movement = sqldf(query)
                        
    print(df_movement.head(20))

    # Convert DataFrame to pivot table
    pivot_table = df_movement.pivot_table(index='Material', columns='month', values='total_value', aggfunc='sum')

    # Reset the index to make 'Material' a column again
    pivot_table = pivot_table.reset_index()

    # Calculate grand total for each material
    pivot_table['Grand_Total'] = pivot_table.iloc[:, 1:].sum(axis=1)

    # Calculate mean and median for each material
    pivot_table['Mean'] = pivot_table.iloc[:, 1:-1].mean(axis=1)
    pivot_table['Median'] = pivot_table.iloc[:, 1:-1].median(axis=1)

    # Export the result DataFrame to a CSV file
    pivot_table.to_csv('dm_movement_pivot.csv', index=False)

    print("Pivot table with mean, median, and grand total has been exported to 'dm_movement_pivot.csv'")

    print(pivot_table.head(20))

    # Create DataFrame
    df = pd.DataFrame(df_invent)

    # Convert Shlf_Date to datetime
    df['Shlf_Date'] = pd.to_datetime(df['Shlf_Date'])

    # Add Expired_Qty column
    df['Expired_Qty'] = df.apply(lambda row: row['TTL'] if row['Shlf_Date'] < cutoff_date else 0, axis=1)

    print(df)

    # Define your SQL query
    query = """
    SELECT Material_1, SUM(TTL) AS OH_TTL, SUM(Expired_Qty) AS Expired
    FROM df
    GROUP BY Material_1
    """
    # Execute the query and create a new DataFrame
    df_expired = sqldf(query) #, globals())

    print(df_expired.head(20))

    # Define your SQL query
    query = """
    SELECT *,
        (ex.OH_TTL - ex.Expired) AS Usable
    FROM 
        pivot_table AS pt
    JOIN 
        df_expired AS ex
    ON 
        pt.Material = ex.Material_1;
    """
    # Execute the query and create a new DataFrame
    df_xxx = sqldf(query) #, globals())

    print(df_xxx.head(20))

    # Export the result DataFrame to a CSV file
    df_xxx.to_csv('dm_movement_pivot_w_expired_qty.csv', index=False)

    import datetime

    # Define the variables
    report_run_date = f"Report Run Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    # Read the existing content of the file
    file_path = 'dm_movement_pivot_w_expired_qty.csv'
    with open(file_path, 'r') as file:
        existing_content = file.read()

    # Create the new content with the additional lines at the top
    new_content = f"Cutoff Date: {cutoff_date}\nDM inventory Raw data CSV file name: \"{dm_invent_csv}\"\nDM Movement Raw data CSV file name: \"{dm_movement_csv}\"\n{report_run_date}\n\n{existing_content}"

    # Write the new content back to the file
    with open(file_path, 'w') as file:
        file.write(new_content)


if __name__ == "__main__":
    
    # Define cutoff date format 'mm/dd/yyyy'
    cutoff_date = '11/19/2024'    
    # Define DM inventory Raw data CSV file name
    dm_invent_csv = 'Au_Wire_Nov_21_2024.XLS.csv'
    # Define DM Movement Raw data CSV file name
    dm_movement_csv = 'movement Jan-Oct_24_120.csv'
    
    dm_mvmnt_result(cutoff_date,dm_invent_csv,dm_movement_csv)


# In[ ]:




