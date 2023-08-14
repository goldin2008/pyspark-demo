from pyspark.sql import SparkSession

# Start SparkSession
spark = SparkSession.builder.appName("managerHierarchyExample").getOrCreate()

# Create DataFrame
data = [("John", "Doe"), ("Jane", "Smith"), ("Mary", "Doe"), 
        ("Mike", "Johnson"), ("Lisa", "Smith"), ("Doe", "Johnson"), 
        ("Emma", "Doe"), ("Jack", "Jane")]
df = spark.createDataFrame(data, ["Associate_Name", "Manager_Name"])

new_df = spark.createDataFrame(df.rdd, df.schema)
new_df.show()
        
# Function to iteratively find all associates reporting (directly or indirectly) to a specific manager
def find_all_associates(df, manager_name):
    # Initial dataframe for the specific manager
    associates_df = df.filter(df.Manager_Name == manager_name)
    associates_df = associates_df.withColumnRenamed("Associate_Name", "Associate_Name_temp")
    associates_df.show()
    count = 1
    while True:
        # Get the associates who report to the managers found in the previous step
        # new_associates_df = df.join(associates_df, df.Manager_Name == associates_df.Associate_Name)
        # Now join the dataframes
        new_df = spark.createDataFrame(df.rdd, df.schema)
        associates_df = associates_df.withColumnRenamed("Associate_Name", "Associate_Name_temp")
        associates_df.show()
        new_associates_df = new_df.join(associates_df, new_df.Manager_Name == associates_df.Associate_Name_temp).drop(new_df.Manager_Name, associates_df.Associate_Name_temp)

        # Remove duplicates
        new_associates_df = new_associates_df.union(associates_df).distinct()

        # If the number of associates does not increase, then stop
        if new_associates_df.count() == associates_df.count():
            associates_df = associates_df.withColumnRenamed("Associate_Name_temp","Associate_Name")
            break
        else:
            associates_df = new_associates_df
            
    return associates_df.select("Associate_Name", "Manager_Name")

# Get all associates reporting to a specific manager
manager_name = "Johnson"
all_associates_df = find_all_associates(df, manager_name)

# Show the dataframe
all_associates_df.show()