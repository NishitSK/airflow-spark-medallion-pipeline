import os
import sys
import json

# We log when sitecustomize executes to help trace boot behavior
print("Bootstrapping Python interpreter: loading sitecustomize.py")

try:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
    
    # 1. Patch SimpleAuthManager.get_users
    def custom_get_users(*args, **kwargs) -> list[dict[str, str]]:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        return [{"username": user, "role": "ADMIN"}]
    
    # 2. Patch SimpleAuthManager.get_passwords
    def custom_get_passwords(*args, **kwargs) -> dict[str, str]:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
        return {user: password}
        
    # 3. Patch SimpleAuthManager.init
    def custom_init(self) -> None:
        user = os.environ.get("AIRFLOW_ADMIN_USER", "admin")
        password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "admin123")
        password_file = self.get_generated_password_file()
        
        # Ensure password file exists and contains the correct JSON credentials
        try:
            os.makedirs(os.path.dirname(password_file), exist_ok=True)
            with open(password_file, "w") as f:
                json.dump({user: password}, f)
        except Exception as e:
            print(f"Simple auth manager | ERROR writing password file: {str(e)}")
            
        # Standard prints so user/process logs match expectations
        print("Simple auth manager | Active Auth Manager: CustomSimpleAuthManager (patched SimpleAuthManager)")
        print(f"Simple auth manager | Configured Airflow User: {user}")
        print("Simple auth manager | Credential Source: .env")
        
    SimpleAuthManager.get_users = staticmethod(custom_get_users)
    SimpleAuthManager.get_passwords = staticmethod(custom_get_passwords)
    SimpleAuthManager.init = custom_init
    
    print("Bootstrapping Python interpreter: successfully patched SimpleAuthManager")
except ImportError:
    # If airflow is not installed in the current python context (e.g. host python or other packages)
    pass
except Exception as e:
    print(f"Bootstrapping Python interpreter: error patching SimpleAuthManager: {str(e)}")
