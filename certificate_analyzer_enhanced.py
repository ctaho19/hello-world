#!/usr/bin/env python3

import argparse
import json
import logging
import sys
import warnings
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Iterator, Any
from urllib3.exceptions import InsecureRequestWarning

import pandas as pd
import requests
import snowflake.connector
import yaml
from dateutil.parser import parse

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

DEFAULT_API_BATCH_SIZE = 10000
SAFETY_LIMIT = 50000
SNOWFLAKE_QUERY = """
    SELECT DISTINCT CERTIFICATE_ARN
    FROM CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE
    WHERE CERTIFICATE_ARN LIKE '%arn:aws:acm:%' 
    AND NOT_VALID_AFTER_UTC_TIMESTAMP >= CURRENT_TIMESTAMP
"""

def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

@dataclass(frozen=True, slots=True)
class Certificate:
    arn: str
    renewal_eligibility: str
    cert_type: str
    renewal_status: str = "UNKNOWN"
    asv_tag: Optional[str] = None
    ba_tag: Optional[str] = None
    not_after: Optional[datetime] = None
    aws_account_id: Optional[str] = None
    aws_region: Optional[str] = None
    aws_service: Optional[str] = None
    environment: Optional[str] = None
    business_application_name: Optional[str] = None
    asv_name: Optional[str] = None
    line_of_business: Optional[str] = None
    domain_name: Optional[str] = None
    in_use: Optional[str] = None
    
    @classmethod
    def from_api_resource(cls, resource: Dict[str, Any]) -> 'Certificate':
        arn = normalize_arn(resource.get('amazonResourceName', ''))
        renewal_eligibility = "UNKNOWN"
        cert_type = "UNKNOWN"
        renewal_status = "UNKNOWN"
        asv_tag = None
        ba_tag = None
        not_after = None
        
        aws_account_id = resource.get('awsAccountId')
        aws_region = resource.get('awsRegion')
        aws_service = resource.get('awsService')
        environment = resource.get('environment')
        business_application_name = resource.get('businessApplicationName')
        asv_name = resource.get('asvName')
        line_of_business = resource.get('lineOfBusiness')
        domain_name = None
        in_use = None
        
        for item in resource.get("configurationItems", []):
            config_name = item.get("configurationName", "")
            config_value = item.get("configurationValue", "")
            
            if config_name == "configuration.renewalEligibility":
                renewal_eligibility = config_value.strip('"')
            elif config_name == "configuration.type":
                cert_type = config_value.strip('"')
            elif config_name == "configuration.renewalStatus":
                renewal_status = config_value.strip('"')
            elif config_name == "configuration.notAfter":
                try:
                    not_after = parse(config_value.strip('"'))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse notAfter '{config_value}' for ARN {arn}: {e}")
            elif config_name == "configuration.domainName":
                domain_name = config_value.strip('"')
            elif config_name == "configuration.inUse":
                in_use = config_value.strip('"')
        
        supp_config = resource.get("supplementaryConfiguration", [])
        if isinstance(supp_config, dict):
            supp_config = [supp_config]
            
        for item in supp_config:
            if item.get("supplementaryConfigurationName") == "Tags":
                try:
                    tags_str = item.get("supplementaryConfigurationValue", "").strip('"')
                    if tags_str:
                        tags = json.loads(tags_str)
                        for tag in tags:
                            if tag.get('key') == 'ASV':
                                asv_tag = tag.get('value')
                            elif tag.get('key') == 'BA':
                                ba_tag = tag.get('value')
                except (json.JSONDecodeError, AttributeError, TypeError):
                    logger.debug(f"Could not parse tags for ARN {arn}")
        
        return cls(
            arn=arn,
            renewal_eligibility=renewal_eligibility,
            cert_type=cert_type,
            renewal_status=renewal_status,
            asv_tag=asv_tag,
            ba_tag=ba_tag,
            not_after=not_after,
            aws_account_id=aws_account_id,
            aws_region=aws_region,
            aws_service=aws_service,
            environment=environment,
            business_application_name=business_application_name,
            asv_name=asv_name,
            line_of_business=line_of_business,
            domain_name=domain_name,
            in_use=in_use
        )

@dataclass
class AnalysisResult:
    total_api_certificates: int
    total_dataset_certificates: int
    matching_certificates: int
    api_only_certificates: int
    dataset_only_arns: Set[str]
    rotating_configs: List[Certificate]
    non_rotating_configs: List[Certificate]
    comparison_df: pd.DataFrame
    insights: Dict[str, Any] = field(default_factory=dict)

def normalize_arn(arn: str) -> str:
    return arn.strip('"').strip() if arn else ""

def load_env_config(env_name: str) -> Tuple[Dict, Dict]:
    script_dir = Path(__file__).parent
    config = {}

    def deep_merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
                deep_merge(value, destination[key])
            else:
                destination[key] = value
        return destination
    
    try:
        base_env_file = script_dir / 'env.yml'
        if base_env_file.exists():
            logger.info(f"Loading base configuration from {base_env_file}")
            with open(base_env_file, 'r') as f:
                config = yaml.safe_load(f) or {}
        else:
            logger.warning(f"Base configuration file not found: {base_env_file}")
        
        override_file = script_dir / 'env.override.yml'
        if override_file.exists():
            logger.info(f"Loading override configuration from {override_file}")
            with open(override_file, 'r') as f:
                override_config = yaml.safe_load(f) or {}
            config = deep_merge(override_config, config)

        env_settings = config.get('settings', {}).get(env_name, {})
        env_secrets = config.get('secrets', {}).get(env_name, {})

        if not env_settings:
            raise ValueError(f"Environment settings for {env_name} not found in configuration files")
        if not env_secrets:
            raise ValueError(f"Environment secrets for {env_name} not found in configuration files")
        
        logger.info(f"Successfully loaded configuration for environment: {env_name}")
        return env_settings, env_secrets
        
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

def get_snowflake_connection(snf_settings: Dict, snf_secrets: Dict) -> snowflake.connector.SnowflakeConnection:
    try:
        creds = snf_secrets.get('values', snf_secrets)
        if 'snowflake' in snf_secrets:
            creds = snf_secrets['snowflake'].get('values', snf_secrets['snowflake'])
            
        if not creds.get('username') or not creds.get('password'):
            raise ValueError("Missing username or password credentials")
        
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=creds.get('username'),
            password=creds.get('password'),
            account=snf_settings['snowflake_account'],
            warehouse=snf_settings['snowflake_warehouse'],
            database='CYBR_DB',
            schema='PHDP_CYBR'
        )
        logger.info("Successfully connected to Snowflake")
        return conn
        
    except snowflake.connector.errors.DatabaseError as e:
        logger.error(f"Snowflake database error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

def get_dataset_arns(conn: snowflake.connector.SnowflakeConnection) -> Set[str]:
    try:
        logger.info("Fetching certificate ARNs from Snowflake dataset...")
        with conn.cursor() as cursor:
            cursor.execute(SNOWFLAKE_QUERY)
            df = cursor.fetch_pandas_all()
            arns = {normalize_arn(arn) for arn in df['CERTIFICATE_ARN'] if arn}
            logger.info(f"Found {len(arns)} certificate ARNs in dataset")
            return arns
    except Exception as e:
        logger.error(f"Error querying Snowflake: {e}")
        raise

def stream_api_certificates(api_settings: Dict, auth_token: str, 
                          batch_size: int = DEFAULT_API_BATCH_SIZE) -> Iterator[Certificate]:
    base_api_url = f"{api_settings['onestream_host']}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
    next_record_key = None
    
    if not auth_token:
        raise ValueError("Authentication token is required for API access")

    try:
        session = requests.Session()
        session.verify = False
        session.timeout = 30

        session.headers.update({
            "Accept": "application/json;v=1.0",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_token}"
        })

        api_payload = {
            "maxResults": batch_size,
            "searchParameters": [{
                "resourceType": "AWS::ACM::Certificate", 
                "configurationItems": [{
                    "configurationName": "status", 
                    "configurationValue": "\"ISSUED\""
                }]
            }]
        }

        now_utc = datetime.now(timezone.utc)
        logger.info("Starting API data stream for certificate configurations...")
        
        page_count = 0
        total_processed = 0
        
        while True:
            page_count += 1
            logger.debug(f"Fetching page {page_count}...")
            
            current_payload = api_payload.copy()
            if next_record_key:
                current_payload['nextRecordKey'] = next_record_key

            response = session.post(base_api_url, json=current_payload)
            response.raise_for_status()
            data = response.json()
            
            resources = data.get("resourceConfigurations", [])
            page_valid_count = 0
            
            for resource in resources:
                total_processed += 1
                try:
                    cert = Certificate.from_api_resource(resource)
                    
                    if cert.not_after and cert.not_after > now_utc:
                        yield cert
                        page_valid_count += 1
                    elif not cert.not_after:
                        yield cert
                        page_valid_count += 1
                        
                except Exception as e:
                    logger.warning(f"Error processing certificate {resource.get('amazonResourceName', 'Unknown')}: {e}")
            
            logger.info(f"Page {page_count}: {page_valid_count}/{len(resources)} valid certificates processed")
            
            next_record_key = data.get("nextRecordKey")
            if not next_record_key:
                logger.info(f"API stream complete: {total_processed} certificates processed")
                break
            else:
                logger.debug(f"Next record key: {next_record_key[:50]}..." if len(next_record_key) > 50 else next_record_key)
            
            if total_processed >= SAFETY_LIMIT:
                logger.warning(f"Stopping at {total_processed} certificates (safety limit)")
                break
                
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error streaming API data: {e}")
        raise

def analyze_certificate_configurations(certificates: List[Certificate]) -> pd.DataFrame:
    if not certificates:
        return pd.DataFrame(columns=['Configuration Field', 'Value', 'Count', 'Percentage'])
    
    analysis_data = {
        'renewal_eligibility': [cert.renewal_eligibility for cert in certificates],
        'cert_type': [cert.cert_type for cert in certificates],
        'renewal_status': [cert.renewal_status for cert in certificates],
        'asv_tag': [cert.asv_tag or "Not Found" for cert in certificates],
        'ba_tag': [cert.ba_tag or "Not Found" for cert in certificates],
        'aws_region': [cert.aws_region or "Not Found" for cert in certificates],
        'environment': [cert.environment or "Not Found" for cert in certificates],
        'business_application_name': [cert.business_application_name or "Not Found" for cert in certificates],
        'asv_name': [cert.asv_name or "Not Found" for cert in certificates],
        'line_of_business': [cert.line_of_business or "Not Found" for cert in certificates],
        'in_use': [cert.in_use or "Not Found" for cert in certificates]
    }
    
    summary_list = []
    total_count = len(certificates)
    
    for field, values in analysis_data.items():
        counts = Counter(values)
        for value, count in counts.items():
            percentage = (count / total_count * 100) if total_count > 0 else 0
            summary_list.append({
                "Configuration Field": field,
                "Value": value,
                "Count": count,
                "Percentage": f"{percentage:.1f}%"
            })
    
    return pd.DataFrame(summary_list)

def create_comparative_analysis(rotating_configs: List[Certificate], 
                              non_rotating_configs: List[Certificate]) -> pd.DataFrame:
    
    rotating_summary = analyze_certificate_configurations(rotating_configs)
    non_rotating_summary = analyze_certificate_configurations(non_rotating_configs)
    
    comparison_data = []
    
    all_fields = set()
    if not rotating_summary.empty:
        all_fields.update(rotating_summary['Configuration Field'].unique())
    if not non_rotating_summary.empty:
        all_fields.update(non_rotating_summary['Configuration Field'].unique())
    
    for field in sorted(all_fields):
        rotating_field_data = rotating_summary[rotating_summary['Configuration Field'] == field] if not rotating_summary.empty else pd.DataFrame()
        non_rotating_field_data = non_rotating_summary[non_rotating_summary['Configuration Field'] == field] if not non_rotating_summary.empty else pd.DataFrame()
        
        all_values = set()
        if not rotating_field_data.empty:
            all_values.update(rotating_field_data['Value'].unique())
        if not non_rotating_field_data.empty:
            all_values.update(non_rotating_field_data['Value'].unique())
        
        for value in sorted(all_values):
            rotating_count = 0
            non_rotating_count = 0
            
            if not rotating_field_data.empty:
                rotating_match = rotating_field_data[rotating_field_data['Value'] == value]
                if not rotating_match.empty:
                    rotating_count = rotating_match['Count'].iloc[0]
            
            if not non_rotating_field_data.empty:
                non_rotating_match = non_rotating_field_data[non_rotating_field_data['Value'] == value]
                if not non_rotating_match.empty:
                    non_rotating_count = non_rotating_match['Count'].iloc[0]
            
            total_rotating = len(rotating_configs)
            total_non_rotating = len(non_rotating_configs)
            
            rotating_pct = (rotating_count / total_rotating * 100) if total_rotating > 0 else 0
            non_rotating_pct = (non_rotating_count / total_non_rotating * 100) if total_non_rotating > 0 else 0
            
            comparison_data.append({
                'Configuration Field': field,
                'Value': value,
                'Rotating Count': rotating_count,
                'Rotating %': f"{rotating_pct:.1f}%",
                'Non-Rotating Count': non_rotating_count,
                'Non-Rotating %': f"{non_rotating_pct:.1f}%",
                'Difference': f"{rotating_pct - non_rotating_pct:+.1f}%"
            })
    
    return pd.DataFrame(comparison_data)

def diagnose_arn_mismatches(dataset_arns: Set[str], api_arns: Set[str]) -> Dict[str, Any]:
    missing_in_api = dataset_arns - api_arns
    missing_in_dataset = api_arns - dataset_arns
    
    diagnosis = {
        'dataset_only_count': len(missing_in_api),
        'api_only_count': len(missing_in_dataset),
        'overlap_count': len(dataset_arns & api_arns),
        'dataset_only_sample': list(missing_in_api)[:10],
        'api_only_sample': list(missing_in_dataset)[:10]
    }
    
    logger.info(f"ARN Match Diagnosis:")
    logger.info(f"  Dataset ARNs: {len(dataset_arns)}")
    logger.info(f"  API ARNs: {len(api_arns)}")
    logger.info(f"  Overlapping ARNs: {diagnosis['overlap_count']}")
    logger.info(f"  Dataset-only ARNs: {diagnosis['dataset_only_count']}")
    logger.info(f"  API-only ARNs: {diagnosis['api_only_count']}")
    
    if diagnosis['dataset_only_count'] > 0:
        logger.info(f"  Sample dataset-only ARNs: {diagnosis['dataset_only_sample']}")
    
    return diagnosis

def generate_rotation_insights(comparison_df: pd.DataFrame, analysis_result: AnalysisResult) -> Dict[str, Any]:
    insights = {}
    
    if comparison_df.empty:
        return insights
    
    comparison_df['Diff_Numeric'] = comparison_df['Difference'].str.replace('%', '').astype(float)
    
    significant_diffs = comparison_df[abs(comparison_df['Diff_Numeric']) > 10].sort_values('Diff_Numeric', ascending=False)
    
    insights['significant_differences'] = significant_diffs.to_dict('records') if not significant_diffs.empty else []
    
    strong_predictors = comparison_df[comparison_df['Diff_Numeric'] > 20]
    rotation_blockers = comparison_df[comparison_df['Diff_Numeric'] < -20]
    
    insights['rotation_predictors'] = strong_predictors.to_dict('records') if not strong_predictors.empty else []
    insights['rotation_blockers'] = rotation_blockers.to_dict('records') if not rotation_blockers.empty else []
    
    total_certs = analysis_result.total_api_certificates
    rotating_certs = analysis_result.matching_certificates
    insights['rotation_rate'] = (rotating_certs / total_certs * 100) if total_certs > 0 else 0
    
    return insights

def print_analysis_summary(result: AnalysisResult) -> None:
    print(f"\n{'='*80}")
    print("CERTIFICATE ROTATION ANALYSIS SUMMARY")
    print(f"{'='*80}")
    
    print(f"\nDataset Overview:")
    print(f"  • Total API certificates (ISSUED): {result.total_api_certificates:,}")
    print(f"  • Total dataset certificates: {result.total_dataset_certificates:,}")
    print(f"  • Matching certificates: {result.matching_certificates:,}")
    print(f"  • Rotation rate: {result.insights.get('rotation_rate', 0):.1f}%")
    
    if result.dataset_only_arns:
        print(f"  • Dataset-only ARNs: {len(result.dataset_only_arns):,}")
    
    insights = result.insights
    if insights.get('significant_differences'):
        print(f"\nSIGNIFICANT CONFIGURATION DIFFERENCES (>10%):")
        print(f"{'Field':<20} {'Value':<25} {'Rotating %':<12} {'Non-Rot %':<12} {'Difference':<12}")
        print("-" * 85)
        for diff in insights['significant_differences'][:10]:
            print(f"{diff['Configuration Field']:<20} {diff['Value']:<25} {diff['Rotating %']:<12} {diff['Non-Rotating %']:<12} {diff['Difference']:<12}")
    
    if insights.get('rotation_predictors'):
        print(f"\nSTRONG ROTATION PREDICTORS (>20% higher in rotating certs):")
        for pred in insights['rotation_predictors']:
            print(f"  • {pred['Configuration Field']} = '{pred['Value']}' ({pred['Difference']} difference)")
    
    if insights.get('rotation_blockers'):
        print(f"\nROTATION BLOCKERS (>20% higher in non-rotating certs):")
        for blocker in insights['rotation_blockers']:
            print(f"  • {blocker['Configuration Field']} = '{blocker['Value']}' ({blocker['Difference']} difference)")

def save_analysis_results(result: AnalysisResult, output_dir: Path) -> None:
    output_dir.mkdir(exist_ok=True)
    
    comparison_csv = output_dir / "certificate_comparison_analysis.csv"
    result.comparison_df.to_csv(comparison_csv, index=False)
    
    rotating_summary = analyze_certificate_configurations(result.rotating_configs)
    non_rotating_summary = analyze_certificate_configurations(result.non_rotating_configs)
    
    rotating_csv = output_dir / "rotating_certificates_detailed.csv"
    non_rotating_csv = output_dir / "non_rotating_certificates_detailed.csv"
    
    rotating_summary.to_csv(rotating_csv, index=False)
    non_rotating_summary.to_csv(non_rotating_csv, index=False)
    
    insights_json = output_dir / "rotation_insights.json"
    with open(insights_json, 'w') as f:
        json.dump(result.insights, f, indent=2, default=str)
    
    logger.info(f"Analysis results saved to {output_dir}")
    logger.info(f"  • Comparison analysis: {comparison_csv}")
    logger.info(f"  • Rotating certificates: {rotating_csv}")
    logger.info(f"  • Non-rotating certificates: {non_rotating_csv}")
    logger.info(f"  • Insights: {insights_json}")

def main():
    parser = argparse.ArgumentParser(
        description="Analyze certificate rotation discrepancies between API and dataset"
    )
    parser.add_argument("--env", type=str, required=True, 
                       help="Environment to use from configuration files")
    parser.add_argument("--token", type=str, 
                       help="Authentication token for API access")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_API_BATCH_SIZE,
                       help=f"API batch size (default: {DEFAULT_API_BATCH_SIZE})")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "results",
                       help="Output directory for results")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    global logger
    logger = setup_logging(args.verbose)
    
    try:
        logger.info(f"Starting certificate rotation analysis for environment: {args.env}")
        
        env_settings, env_secrets = load_env_config(args.env)
        
        auth_token = args.token or ""
        if not auth_token:
            logger.warning("No authentication token provided. API calls may fail.")
        
        snf_conn = get_snowflake_connection(env_settings, env_secrets)
        dataset_arns = get_dataset_arns(snf_conn)
        snf_conn.close()
        
        logger.info("Streaming API certificate configurations...")
        api_certificates = list(stream_api_certificates(env_settings, auth_token, args.batch_size))
        api_arns = {cert.arn for cert in api_certificates if cert.arn}
        
        arn_diagnosis = diagnose_arn_mismatches(dataset_arns, api_arns)
        
        rotating_configs = [cert for cert in api_certificates if cert.arn in dataset_arns]
        non_rotating_configs = [cert for cert in api_certificates if cert.arn not in dataset_arns]
        
        comparison_df = create_comparative_analysis(rotating_configs, non_rotating_configs)
        
        analysis_result = AnalysisResult(
            total_api_certificates=len(api_certificates),
            total_dataset_certificates=len(dataset_arns),
            matching_certificates=len(rotating_configs),
            api_only_certificates=len(non_rotating_configs),
            dataset_only_arns=dataset_arns - api_arns,
            rotating_configs=rotating_configs,
            non_rotating_configs=non_rotating_configs,
            comparison_df=comparison_df
        )
        
        analysis_result.insights = generate_rotation_insights(comparison_df, analysis_result)
        analysis_result.insights['arn_diagnosis'] = arn_diagnosis
        
        print_analysis_summary(analysis_result)
        
        if not comparison_df.empty and len(comparison_df) <= 50:
            print(f"\n{'='*80}")
            print("DETAILED CONFIGURATION COMPARISON")
            print(f"{'='*80}")
            print(comparison_df.to_string(index=False))
        
        save_analysis_results(analysis_result, args.output_dir)
        
        logger.info("Certificate rotation analysis completed successfully")
        
        if analysis_result.matching_certificates < 100:
            logger.warning("Very low certificate match count detected")
            return 2
        
        return 0
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
