# 1) Combien de lignes contient ce fichier ?
# 2) Combien y a t-il de requêtes par IP en moyenne ?
# 3) Une session est un ensemble de requêtes provenant d'une même IP qui sont espacées au plus de 30 minutes.
# a) Combien y a t-il de sessions en moyenne par jour ?
# b) Combien y a-t-il de requêtes par session ?

import subprocess
import pandas as pd

# Specify the path to the CSV file
csv_file_path = "access.log"

# Get the number of lines in the CSV file
process = subprocess.Popen(['wc', '-l', csv_file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
output, error = process.communicate()
num_lines = int(output.strip().split()[0])

# Define column names
column_names = ['IP', 'identity', 'user', 'timestamp', 'request', 'status', 'size', 'referrer', 'user_agent', 'endminus']
combined_df = pd.DataFrame()
chunk_size = 1000000 #Adapt to best suit your computer performances

# Load the data into a pandas DataFrame
for chunk in pd.read_csv(csv_file_path, sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', engine='python', header=None, chunksize=chunk_size, quotechar='"', on_bad_lines="skip"):
    # Assign meaningful column names to the chunk
    chunk.columns = column_names
    try:
        # Drop unnecessary columns and assign datetime
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'].str.strip('[]'), format='%d/%b/%Y:%H:%M:%S %z',dayfirst=True, errors='coerce')
        chunk.drop(['identity', 'user', 'request', 'status', 'size', 'referrer', 'user_agent', 'endminus'], axis=1, inplace=True)

        # Concatenate the processed chunk with the combined DataFrame
        combined_df = pd.concat([combined_df, chunk])
    except pd.errors.ParserError:
        continue
combined_df = combined_df.dropna()
print("First overview")
print(combined_df.head(10))


print("1) \nNombre de lignes contenu dans le fichier : " + str(num_lines) + "\nPourcentage de lignes skip : " + str((num_lines - combined_df.shape[0]) / num_lines * 100) + "%\n")
print("2) \nNombre de requêtes par IP en moyenne : " + str(combined_df.groupby('IP')['IP'].count().mean()) + "\n")
print("3) \nUne session est un ensemble de requêtes provenant d'une même IP qui sont espacées au plus de 30 minutes. \n")

# Convert timestamp to datetime and sort by IP and timestamp (redo because chunk is not enough)
combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], dayfirst=True,utc=True)
combined_df.sort_values(by=['IP', 'timestamp'], inplace=True)

# Calculate time differences between consecutive requests for each IP
combined_df['time_diff'] = combined_df.groupby('IP')['timestamp'].diff()

# Identify session boundaries based on the time difference threshold
combined_df['session'] = (combined_df['time_diff'] > pd.Timedelta(minutes=30)).cumsum()

# Calculate the average number of sessions per day
avg_sessions_per_day = combined_df.groupby(combined_df['timestamp'].dt.date)['session'].nunique().mean()
print("a) \nNombre de sessions en moyenne par jour : " + str(avg_sessions_per_day) + "\n")

# Calculate the average number of requests per session
avg_requests_per_session = combined_df.groupby('session')['IP'].count().mean()
print("b) \nNombre de requêtes par session : " + str(avg_requests_per_session) + "\n")

print("Last overview")
print(combined_df.head(10))
