CREATE USER mcd_deployment_user IDENTIFIED BY '<strong_password>';
GRANT CREATE, ALTER, INSERT, UPDATE, DELETE ON <schema_name>.* TO mcd_deployment_user;
