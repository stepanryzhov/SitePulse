\getenv monitor_password MONITOR_DB_PASSWORD
\getenv notifier_password NOTIFIER_DB_PASSWORD
CREATE USER monitor WITH PASSWORD :'monitor_password';
CREATE USER notifier WITH PASSWORD :'notifier_password';
CREATE DATABASE monitor_db OWNER monitor;
CREATE DATABASE notifier_db OWNER notifier;
REVOKE CONNECT ON DATABASE monitor_db FROM PUBLIC;
REVOKE CONNECT ON DATABASE notifier_db FROM PUBLIC;
GRANT CONNECT ON DATABASE monitor_db TO monitor;
GRANT CONNECT ON DATABASE notifier_db TO notifier;
