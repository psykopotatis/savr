import requests
import pandas as pd
from tabulate import tabulate

# nexam
company_id = "01934971-a3a6-700f-9af2-eb3153f97d46"
# nanologica
# company_id = "01934971-ebc2-719e-9ebe-90b0749f7de6"

# URLs for holdings
holdings_urls = [
    "https://iu.api.savr.com/companies-v2/%s/holdings?page=1&pageSize=25" % company_id,
    "https://iu.api.savr.com/companies-v2/%s/holdings?page=2&pageSize=25" % company_id
]

agents_base_url = "https://iu.api.savr.com/agents"

def fetch_data(url):
    """Fetch JSON data from the URL."""
    try:
        print("GET " + url)
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# Fetch and combine holdings data
holdings_data = []
for url in holdings_urls:
    data = fetch_data(url)
    if data and isinstance(data, dict) and "data" in data:
        holdings_data.extend(data["data"])

# Collect all unique agentIds from holdings data
all_agent_ids = set()
for holding in holdings_data:
    if "agentId" in holding:
        all_agent_ids.add(holding["agentId"])

# Construct URL to fetch all agents dynamically
agents_query = "&".join([f"id%5B%5D={agent_id}" for agent_id in all_agent_ids])
agents_url = f"{agents_base_url}?{agents_query}"

# Fetch agent data
agents_data = []
agents_response = fetch_data(agents_url)
if agents_response:
    agents_data.extend(agents_response)

# Map holdings to agents, using only the latest holdings entry for each agent
agent_holdings = []
for holding in holdings_data:
    agent_id = holding.get("agentId")
    if "holdings" in holding and holding["holdings"]:
        latest_holding = holding["holdings"][0]  # Assuming the first entry is the latest
        agent_holdings.append({
            "agentId": agent_id,
            "numberOfShares": latest_holding["numberOfShares"],
            "percentageOfShares": latest_holding["percentageOfShares"],
            "numberOfVotes": latest_holding["numberOfVotes"],
            "percentageOfVotes": latest_holding["percentageOfVotes"],
            "date": latest_holding["date"]
        })

# Combine with agent details
agent_details = {agent["id"]: agent for agent in agents_data}
for holding in agent_holdings:
    agent_id = holding["agentId"]
    holding["agentName"] = agent_details.get(agent_id, {}).get("name", "Unknown")
    holding["countryCode"] = agent_details.get(agent_id, {}).get("countryCode", "Unknown")

# Create a DataFrame
df = pd.DataFrame(agent_holdings)

# Remove columns from the DataFrame
columns_to_remove = ["agentId", "numberOfVotes", "percentageOfVotes"]
df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])

# Reorder columns to move 'agentName' to the first and 'countryCode' to the second position
columns_order = ["agentName", "countryCode"] + [col for col in df.columns if col not in ["agentName", "countryCode"]]
df = df[columns_order]

# Display the DataFrame
# import ace_tools as tools; tools.display_dataframe_to_user(name="Agent Holdings with Details", dataframe=df)
# Save the DataFrame to a CSV file
df.to_csv("latest_agent_holdings_with_details.csv", index=False)

# Print a confirmation message
print("The latest data has been saved to 'latest_agent_holdings_with_details.csv'.")

# Convert DataFrame to a tabulated string and print it
# the index to start at 1 instead of 0
df.index = range(1, len(df) + 1)
table = tabulate(df, headers="keys", tablefmt="plain", showindex=True)
print(table)