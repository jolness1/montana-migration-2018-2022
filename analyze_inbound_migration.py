import pandas as pd
from pathlib import Path

def load_data():
    
    # load the migration data and states list
    df = pd.read_csv('data/2018-2022-american-community-survey-MT.csv')
    with open('data/states.txt', 'r') as f:
        states = [line.strip() for line in f.readlines()]
    
    # exclude Montana to focus on migration from other places
    df = df[df['originState'].str.strip().str.lower() != 'montana']

    return df, states

def create_output_directories():
    """Create output directories if they don't exist"""
    Path('output').mkdir(exist_ok=True)
    Path('output/by-county').mkdir(exist_ok=True)

def analyze_overall_migration(df):
    
    # migration to Montana by state/region
    # group by originState and sum inboundFromState
    migration_by_origin = df.groupby('originState', sort=False)['inboundFromState'].sum().reset_index()
    
    # sort by inboundFromState in descending order
    migration_by_origin = migration_by_origin.sort_values('inboundFromState', ascending=False)
    # save to CSV
    migration_by_origin.to_csv('output/montana-migration.csv', index=False)
    
    print("Analysis 1 complete: Saved overall migration data to output/montana-migration.csv")
    print("Top 10 origin states/regions:")
    print(migration_by_origin.head(10))
    print()

def analyze_migration_by_county(df, states):
    
    # migration by county with internal vs external breakdown
    county_data = []
    
    # get unique counties
    counties = df['county'].unique()
    
    for county in counties:
        county_df = df[df['county'] == county]
        
        # calculate US internal vs external migration
        internal_migration = 0
        external_migration = 0
        
        for _, row in county_df.iterrows():
            origin = row['originState']
            migration = row['inboundFromState']
            
            if origin in states:
                internal_migration += migration
            else:
                external_migration += migration
        
        total_migration = internal_migration + external_migration
        
        # calculate percentages
        pct_internal = (internal_migration / total_migration * 100) if total_migration > 0 else 0
        pct_external = (external_migration / total_migration * 100) if total_migration > 0 else 0
        
        county_data.append({
            'county': county,
            'totalMigration': total_migration,
            'totalInternalMigration': internal_migration,
            'totalExternalMigration': external_migration,
            'pctInternal': round(pct_internal, 2),
            'pctExternal': round(pct_external, 2)
        })
    
    # sort by totalMigration
    county_migration_df = pd.DataFrame(county_data)
    county_migration_df = county_migration_df.sort_values('totalMigration', ascending=False).reset_index(drop=True)
    
    # save to CSV
    county_migration_df.to_csv('output/total-migration-by-county.csv', index=False)
    
    print("Analysis 2 complete: Saved county migration data to output/total-migration-by-county.csv")
    print("Top 10 counties by total migration:")
    print(county_migration_df.head(10))
    print()

    return county_migration_df

def analyze_by_county_detail(df, county_migration_df):
    # detailed migration breakdown for each county
    # use the ranked county_migration_df to name files with their rank
    for idx, row in county_migration_df.reset_index().iterrows():
        rank = idx + 1
        county = row['county']
        county_df = df[df['county'] == county]

        # calculate total migration for this county (MT already )
        total_county_migration = county_df['inboundFromState'].sum()

        county_detail = []
        for _, r in county_df.iterrows():
            origin = r['originState']
            migration = r['inboundFromState']
            pct_of_total = (migration / total_county_migration * 100) if total_county_migration > 0 else 0
            county_detail.append({
                'originState': origin,
                'inboundFromState': migration,
                'pctOfTotal': round(pct_of_total, 2)
            })

        county_detail_df = pd.DataFrame(county_detail)
        county_detail_df = county_detail_df.sort_values('inboundFromState', ascending=False)

        # create filename with ranking prefix (ie 1-gallatin.csv, 2-yellowstone.csv etc.)
        filename = f"{rank}-{county.lower().replace(' ', '-')}.csv"
        filepath = f'output/by-county/{filename}'

        county_detail_df.to_csv(filepath, index=False)
    
    print("Analysis 3 complete: Saved detailed county files to output/by-county/")
    print(f"Generated {len(county_migration_df)} county-specific CSV files")

def main():
    print("Starting Montana Migration Analysis...")
    print("=" * 50)
    
    df, states = load_data()
    print(f"Loaded data: {len(df)} rows, {len(df['county'].unique())} counties")
    print(f"Loaded {len(states)} US states/territories from states.txt")
    print()
    
    create_output_directories()
    
    # invoke functions
    analyze_overall_migration(df)
    county_migration_df = analyze_migration_by_county(df, states)
    analyze_by_county_detail(df, county_migration_df)
    
    print("=" * 50)
    print("Analysis complete! Check the output/ directory for results.")

if __name__ == "__main__":
    main()
