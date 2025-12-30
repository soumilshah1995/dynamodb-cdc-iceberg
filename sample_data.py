"""
Sample script to insert user profiles into DynamoDB
Demonstrates flat fields and complex nested properties
"""

import boto3
import time
from decimal import Decimal
from datetime import datetime

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
table_name = 'dev-user-profiles'  # Update with your table name
table = dynamodb.Table(table_name)


def insert_sample_users():
    """Insert sample user profiles with nested properties"""

    users = [
        {
            'user_id': 'user_001',
            'created_at': int(time.time() * 1000),
            # Flat fields
            'email': 'john.doe@example.com',
            'username': 'johndoe',
            'status': 'active',
            'plan_type': 'premium',
            'registration_date': '2024-01-15',
            # Complex nested properties
            'properties': {
                'profile': {
                    'first_name': 'John',
                    'last_name': 'Doe',
                    'age': 32,
                    'bio': 'Software engineer passionate about data',
                    'avatar_url': 'https://example.com/avatars/john.jpg'
                },
                'preferences': {
                    'theme': 'dark',
                    'language': 'en',
                    'notifications': {
                        'email': True,
                        'push': True,
                        'sms': False
                    },
                    'privacy': {
                        'profile_public': True,
                        'show_email': False,
                        'analytics_opt_in': True
                    }
                },
                'subscription': {
                    'plan': 'premium',
                    'billing_cycle': 'annual',
                    'price': Decimal('99.99'),
                    'currency': 'USD',
                    'next_billing_date': '2025-01-15',
                    'features': [
                        'unlimited_storage',
                        'priority_support',
                        'advanced_analytics',
                        'api_access'
                    ]
                },
                'usage_stats': {
                    'total_logins': 1234,
                    'last_login': '2024-12-29T10:30:00Z',
                    'storage_used_gb': Decimal('45.67'),
                    'api_calls_month': 15890,
                    'active_projects': 12
                },
                'metadata': {
                    'source': 'web_signup',
                    'referrer': 'google_ads',
                    'campaign_id': 'winter_2024',
                    'device_info': {
                        'type': 'desktop',
                        'os': 'macOS',
                        'browser': 'Chrome'
                    }
                }
            }
        },
        {
            'user_id': 'user_002',
            'created_at': int(time.time() * 1000),
            'email': 'jane.smith@example.com',
            'username': 'janesmith',
            'status': 'active',
            'plan_type': 'free',
            'registration_date': '2024-06-20',
            'properties': {
                'profile': {
                    'first_name': 'Jane',
                    'last_name': 'Smith',
                    'age': 28,
                    'bio': 'Data analyst and visualization enthusiast',
                    'avatar_url': 'https://example.com/avatars/jane.jpg'
                },
                'preferences': {
                    'theme': 'light',
                    'language': 'en',
                    'notifications': {
                        'email': True,
                        'push': False,
                        'sms': False
                    },
                    'privacy': {
                        'profile_public': False,
                        'show_email': False,
                        'analytics_opt_in': False
                    }
                },
                'subscription': {
                    'plan': 'free',
                    'billing_cycle': None,
                    'price': Decimal('0.00'),
                    'currency': 'USD',
                    'features': [
                        'basic_storage',
                        'community_support'
                    ]
                },
                'usage_stats': {
                    'total_logins': 89,
                    'last_login': '2024-12-28T15:45:00Z',
                    'storage_used_gb': Decimal('2.34'),
                    'api_calls_month': 450,
                    'active_projects': 3
                },
                'metadata': {
                    'source': 'mobile_app',
                    'referrer': 'app_store',
                    'device_info': {
                        'type': 'mobile',
                        'os': 'iOS',
                        'browser': 'Safari'
                    }
                }
            }
        },
        {
            'user_id': 'user_003',
            'created_at': int(time.time() * 1000),
            'email': 'bob.wilson@example.com',
            'username': 'bobwilson',
            'status': 'trial',
            'plan_type': 'enterprise_trial',
            'registration_date': '2024-12-15',
            'properties': {
                'profile': {
                    'first_name': 'Bob',
                    'last_name': 'Wilson',
                    'age': 45,
                    'bio': 'CTO at TechCorp',
                    'avatar_url': 'https://example.com/avatars/bob.jpg'
                },
                'preferences': {
                    'theme': 'dark',
                    'language': 'en',
                    'notifications': {
                        'email': True,
                        'push': True,
                        'sms': True
                    },
                    'privacy': {
                        'profile_public': True,
                        'show_email': True,
                        'analytics_opt_in': True
                    }
                },
                'subscription': {
                    'plan': 'enterprise_trial',
                    'billing_cycle': 'monthly',
                    'price': Decimal('499.99'),
                    'currency': 'USD',
                    'trial_ends': '2025-01-15',
                    'features': [
                        'unlimited_storage',
                        'priority_support',
                        'advanced_analytics',
                        'api_access',
                        'sso',
                        'audit_logs',
                        'dedicated_support'
                    ]
                },
                'usage_stats': {
                    'total_logins': 45,
                    'last_login': '2024-12-29T08:15:00Z',
                    'storage_used_gb': Decimal('12.89'),
                    'api_calls_month': 5670,
                    'active_projects': 8,
                    'team_size': 25
                },
                'metadata': {
                    'source': 'sales_demo',
                    'referrer': 'sales_team',
                    'sales_rep': 'rep_456',
                    'company': 'TechCorp Inc',
                    'device_info': {
                        'type': 'desktop',
                        'os': 'Windows',
                        'browser': 'Edge'
                    }
                }
            }
        }
    ]

    for user in users:
        try:
            response = table.put_item(Item=user)
            print(f"✓ Inserted user: {user['user_id']} ({user['email']})")
        except Exception as e:
            print(f"✗ Error inserting user {user['user_id']}: {str(e)}")


def update_user_example():
    """Example of updating a user to trigger CDC"""
    try:
        response = table.update_item(
            Key={
                'user_id': 'user_001',
                'created_at': int(time.time() * 1000)  # You'll need the actual created_at
            },
            UpdateExpression='SET properties.usage_stats.total_logins = properties.usage_stats.total_logins + :inc',
            ExpressionAttributeValues={
                ':inc': 1
            }
        )
        print("✓ Updated user_001 login count")
    except Exception as e:
        print(f"✗ Error updating user: {str(e)}")


if __name__ == '__main__':
    print("Inserting sample user profiles...")
    insert_sample_users()
    print("\nDone! Check your DynamoDB table and S3 bucket for CDC events.")