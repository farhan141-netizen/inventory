import pandas as pd
from st_supabase_connection import SupabaseConnection

class Restaurant:
    def __init__(self):
        self.conn = SupabaseConnection().get_connection()
        
    def load_from_sheet(self, table_name, default_cols=None):
        # Load data from Supabase table
        if default_cols:
            return self.conn.table(table_name).select(*default_cols).execute()
        return self.conn.table(table_name).select().execute()

    def save_to_sheet(self, df, table_name):
        # Use upsert to save DataFrame to Supabase table
        for index, row in df.iterrows():
            self.conn.table(table_name).upsert(row.to_dict()).execute()
        
    def handle_received_items(self):
        # Existing logic for handling received items
        pass

    def check_accepted_qty(self, item):
        # Existing logic for AcceptedQty prevention
        pass

# Existing code logic continued here...