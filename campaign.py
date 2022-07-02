#!/usr/bin/env python
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This example illustrates how to get all campaigns.

To add campaigns, run add_campaigns.py.
"""


import argparse
import sys

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import pandas as pd
from pathlib import Path 

def main(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          segments.date,  
          campaign.id,  
          campaign.name,
          metrics.clicks,
          metrics.impressions,
          metrics.average_cpc,
          metrics.ctr,
          campaign.optimization_score,
          campaign.target_cpa.target_cpa_micros,
          campaign.bidding_strategy_type,
          metrics.conversions, 
          metrics.conversions_value,
          metrics.cost_per_conversion,
          campaign.app_campaign_setting.bidding_strategy_goal_type,
          metrics.cost_micros,
          metrics.sk_ad_network_conversions,
          campaign.advertising_channel_type,
          metrics.average_time_on_site,
          campaign.status
        FROM 
            campaign
        WHERE metrics.clicks > 1
        AND segments.date DURING LAST_MONTH
        LIMIT 
            1000
        """

    # Issues a search request using streaming.
    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    all_data = []

    for batch in stream:    
        for row in batch.results:
            single_row = dict()
            single_row["date"] = row.segments.date
            single_row["campaign_id"] = row.campaign.id
            single_row["campaign_name"] = row.campaign.name
            single_row["clicks"] = row.metrics.clicks
            single_row["impressions"] = row.metrics.impressions
            single_row["cpc"] = row.metrics.average_cpc
            single_row["ctr"] = row.metrics.ctr
            single_row["opt_score"] = row.campaign.optimization_score   
            single_row["tar_cpa"] = row.campaign.target_cpa.target_cpa_micros
            single_row["bid_strategy_type"] = row.campaign.bidding_strategy_type
            single_row["conversions"] = row.metrics.conversions
            single_row["conversions_value"] = row.metrics.conversions_value
            single_row["cost_per_conversion"] = row.metrics.cost_per_conversion
            single_row["app_objective"] = row.campaign.app_campaign_setting.bidding_strategy_goal_type
            single_row["spend"] = row.metrics.cost_micros
            single_row["Skad_conversions"] = row.metrics.sk_ad_network_conversions
            single_row["channel type"] = row.campaign.advertising_channel_type
            single_row["time_on_app"] = row.metrics.average_time_on_site
            single_row["status"] = row.campaign.status
            all_data.append(single_row)
            #print(all_data)
            df = pd.DataFrame(all_data)
            print(df)
            import os
            path = "/Users/gustavo/Desktop/google_ads/"
            output_file = os.path.join(path,'test_gus.csv')
            df.to_csv(output_file, index=False)

if __name__ == "__main__":
    # GoogleAdsClient will read the google-ads.yaml configuration file in the
    # home directory if none is specified.
    googleads_client = GoogleAdsClient.load_from_storage("/Users/gustavo/Desktop/google_ads/google-ads.yaml")  




    parser = argparse.ArgumentParser(
        description="Lists all campaigns for specified customer."
    )
    # The following argument(s) should be provided to run the example.
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=False,
        help="The Google Ads customer ID.",
    )
    args = parser.parse_args()

    try:
        main(googleads_client, args.customer_id)
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)         
