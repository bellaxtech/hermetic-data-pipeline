# Kerberos Authentication Setup for Hadoop / Hive Security

> **Purpose**: This document explains Kerberos authentication configuration for secured
> Hadoop and Hive clusters — essential for the hermetic data pipeline when deploying
> into enterprise environments with Kerberos-enabled HDFS, Hive Metastore, or Spark.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Kerberos Basics](#kerberos-basics)
4. [Setup Steps](#setup-steps)
   - [4.1 Install Kerberos Client](#41-install-kerberos-client)
   - [4.2 Configure krb5.conf](#42-configure-krb5conf)
   - [4.3 Obtain Ticket via kinit](#43-obtain-ticket-via-kinit)
   - [4.4 Using Keytab for Automated Authentication](#44-using-keytab-for-automated-authentication)
5. [Hadoop / HDFS Configuration](#45-hadoop--hdfs-configuration)
6. [Hive Metastore Configuration](#46-hive-metastore-configuration)
7. [Spark with Kerberos](#47-spark-with-kerberos)
8. [Airflow with Kerberos](#48-airflow-with-kerberos)
9. [Troubleshooting](#5-troubleshooting)
10. [Security Best Practices](#6-security-best-practices)

---

## Overview

Kerberos is an authentication protocol that uses tickets to allow nodes
communicating over a non-secure network to prove their identity securely.
In Hadoop ecosystems, Kerberos is the standard mechanism for securing:

- **HDFS** — Authenticating users accessing the filesystem
- **Hive Metastore** — Securing metadata access
- **YARN** — Authenticating job submissions
- **Spark** — Authenticated communication with cluster components
- **HBase / Kafka / ZooKeeper** — Additional ecosystem security

Without Kerberos, any user can impersonate any other user. With Kerberos,
all principals (users/services) must prove their identity via a trusted
third party called the **KDC** (Key Distribution Center).

---

## Prerequisites

Before configuring Kerberos, ensure you have:

| Component | Requirement |
|-----------|-------------|
| **KDC**    | Accessible Kerberos Key Distribution Center (e.g., MIT Kerberos, Active Directory) |
| **Realm**  | Kerberos realm name (e.g., `EXAMPLE.COM`) |
| **Principal** | Service or user principal (e.g., `hive/hostname@REALM`) |
| **Keytab**  | Keytab file containing encrypted principal credentials |
| **Network** | Connectivity to KDC on ports 88 (UDP/TCP) and 749 (TCP admin) |

---

## Kerberos Basics

### Key Concepts

| Term | Description |
|------|-------------|
| **Principal** | Unique identity in Kerberos: `primary/instance@REALM` |
| **Realm** | Kerberos domain namespace (uppercase, e.g., `EXAMPLE.COM`) |
| **TGT** | Ticket-Granting Ticket — initial authentication token |
| **Keytab** | File containing encrypted principal credentials (passwordless auth) |
| **kinit** | Command to obtain and cache TGT |
| **klist** | Command to list cached Kerberos tickets |
| **kdestroy** | Command to destroy cached tickets |

### Authentication Flow

```
User/Service                    KDC                    Service (HDFS/Hive)
    |                            |                          |
    |--- kinit (password) ------>|                          |
    |<--- TGT (ticket) ----------|                          |
    |                            |                          |
    |--- Request service ticket -|-> (using TGT)            |
    |<--- Service ticket --------|                          |
    |                            |                          |
    |--- Authenticate (ticket) -->------------------------->|
    |<-- Granted access -----------------------------------|
```

---

## Setup Steps

### 4.1 Install Kerberos Client

```bash
# Debian / Ubuntu
sudo apt-get update
sudo apt-get install -y krb5-user krb5-config libkrb5-dev

# RHEL / CentOS / Rocky
sudo yum install -y krb5-workstation krb5-libs

# Verify installation
kinit -V
klist -V
```

### 4.2 Configure krb5.conf

The main Kerberos configuration file is `/etc/krb5.conf`. Below is a typical
enterprise configuration:

```ini
[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = true
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true
    proxiable = true
    rdns = false
    default_ccache_name = KEYRING:persistent:%{uid}
    # Alternative for non-keyring environments:
    # default_ccache_name = FILE:/tmp/krb5cc_%{uid}

[realms]
    EXAMPLE.COM = {
        kdc = kdc01.corp.example.com
        kdc = kdc02.corp.example.com
        admin_server = kdc01.corp.example.com
        default_domain = corp.example.com
    }

[domain_realm]
    .corp.example.com = EXAMPLE.COM
    corp.example.com = EXAMPLE.COM

[logging]
    kdc = FILE:/var/log/krb5kdc.log
    admin_server = FILE:/var/log/kadmin.log
    default = FILE:/var/log/krb5lib.log
```

**Important fields:**

- `default_realm` — must match your Kerberos realm (all uppercase)
- `kdc` — KDC server hostnames (use multiple for HA)
- `ticket_lifetime` — how long a TGT is valid
- `renew_lifetime` — maximum renewable period

### 4.3 Obtain Ticket via kinit

Manual authentication using a password:

```bash
# kinit with username@REALM
kinit bella@EXAMPLE.COM
# Password: ********

# Verify ticket
klist

# Output example:
# Ticket cache: FILE:/tmp/krb5cc_501
# Default principal: bella@EXAMPLE.COM
#
# Valid starting     Expires            Service principal
# 01/15/24 09:00:00  01/16/24 09:00:00  krbtgt/EXAMPLE.COM@EXAMPLE.COM
#         renew until 01/22/24 09:00:00
```

### 4.4 Using Keytab for Automated Authentication

A **keytab** (key table) file stores encrypted principal credentials and allows
automated, password-less authentication — essential for services and scheduled jobs.

#### Generating a Keytab

Ask your Kerberos admin to create a keytab, or use `kadmin`:

```bash
# Using kadmin.local (on KDC)
sudo kadmin.local -q "addprinc -randkey bella/airflow@EXAMPLE.COM"
sudo kadmin.local -q "ktadd -k /etc/security/airflow.keytab bella/airflow@EXAMPLE.COM"

# Using remote kadmin
kadmin -p admin/admin@EXAMPLE.COM
kadmin:  addprinc -randkey pipeline-svc@EXAMPLE.COM
kadmin:  ktadd -k /etc/security/pipeline.keytab pipeline-svc@EXAMPLE.COM
```

#### Authenticating with a Keytab

```bash
# One-time login
kinit -kt /etc/security/pipeline.keytab pipeline-svc@EXAMPLE.COM

# Verify
klist

# Automate in cron/systemd — check before using
if ! klist -s; then
    kinit -kt /etc/security/pipeline.keytab pipeline-svc@EXAMPLE.COM
fi
```

**Keytab file security** — keytabs contain secret credentials:

```bash
sudo chown pipeline-svc:pipeline-svc /etc/security/pipeline.keytab
sudo chmod 400 /etc/security/pipeline.keytab  # owner read-only
```

### 4.5 Hadoop / HDFS Configuration

Core Hadoop configuration for Kerberos authentication:

**core-site.xml:**
```xml
<configuration>
    <property>
        <name>hadoop.security.authentication</name>
        <value>kerberos</value>
    </property>
    <property>
        <name>hadoop.security.authorization</name>
        <value>true</value>
    </property>
</configuration>
```

**hdfs-site.xml (NameNode):**
```xml
<configuration>
    <property>
        <name>dfs.namenode.kerberos.principal</name>
        <value>nn/_HOST@EXAMPLE.COM</value>
    </property>
    <property>
        <name>dfs.namenode.keytab.file</name>
        <value>/etc/security/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.datanode.kerberos.principal</name>
        <value>dn/_HOST@EXAMPLE.COM</value>
    </property>
    <property>
        <name>dfs.datanode.keytab.file</name>
        <value>/etc/security/hdfs.keytab</value>
    </property>
    <property>
        <name>dfs.web.authentication.kerberos.principal</name>
        <value>HTTP/_HOST@EXAMPLE.COM</value>
    </property>
</configuration>
```

### 4.6 Hive Metastore Configuration

**hive-site.xml:**
```xml
<configuration>
    <property>
        <name>hive.metastore.kerberos.principal</name>
        <value>hive/_HOST@EXAMPLE.COM</value>
    </property>
    <property>
        <name>hive.metastore.kerberos.keytab.file</name>
        <value>/etc/security/hive.keytab</value>
    </property>
    <property>
        <name>hive.metastore.sasl.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.security.authorization.enabled</name>
        <value>true</value>
    </property>
</configuration>
```

### 4.7 Spark with Kerberos

#### SparkSession Configuration

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("KerberosPipelineJob")
    # Kerberos config for Hive Metastore
    .config("spark.sql.hive.metastore.jars", "builtin")
    .config("hive.metastore.uris", "thrift://hive-metastore.corp.example.com:9083")
    .config("hive.metastore.sasl.enabled", "true")
    .config(
        "hive.metastore.kerberos.principal",
        "hive/_HOST@EXAMPLE.COM",
    )
    # Spark security
    .config("spark.yarn.principal", "pipeline-svc@EXAMPLE.COM")
    .config("spark.yarn.keytab", "/etc/security/pipeline.keytab")
    .config("spark.kerberos.access.hadoopFileSystems", "hdfs://namenode.corp.example.com:8020")
    .config("spark.hadoop.hadoop.security.authentication", "kerberos")
    .enableHiveSupport()
    .getOrCreate()
)
```

#### Spark-Submit Command

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --principal pipeline-svc@EXAMPLE.COM \
    --keytab /etc/security/pipeline.keytab \
    --conf spark.yarn.access.hadoopFileSystems=hdfs://namenode.corp.example.com:8020 \
    --conf spark.sql.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM \
    --jars iceberg-spark-runtime-3.4_2.12-1.3.1.jar \
    spark/jobs/iceberg_schema_evolution.py
```

#### Kerberos Key Tab for Spark

For long-running Spark Streaming jobs, enable keytab renewal:

```bash
spark-submit \
    --conf spark.yarn.keytab=/etc/security/pipeline.keytab \
    --conf spark.yarn.principal=pipeline-svc@EXAMPLE.COM \
    --conf spark.kerberos.renewal.interval=86400 \
    --conf spark.kerberos.ticket.renewal.retries=3
```

### 4.8 Airflow with Kerberos

#### Docker Compose Kerberos Setup

```yaml
version: "3.8"
services:
  airflow-worker:
    image: apache/airflow:2.8.0
    environment:
      # Kerberos
      KRB5_CONFIG: /etc/krb5.conf
      KRB5CCNAME: FILE:/tmp/krb5cc
      # Hadoop
      HADOOP_OPTS: "-Djava.security.krb5.conf=/etc/krb5.conf"
    volumes:
      - /etc/krb5.conf:/etc/krb5.conf:ro
      - /etc/security/airflow.keytab:/etc/security/airflow.keytab:ro
    command: >
      sh -c "
        kinit -kt /etc/security/airflow.keytab airflow-svc@EXAMPLE.COM &&
        airflow celery worker
      "
```

#### Airflow Kerberos Connection

In Airflow UI or `airflow_connections.json`:

```json
{
    "conn_id": "hive_metastore_kerberos",
    "conn_type": "hive_metastore",
    "host": "hive-metastore.corp.example.com",
    "port": 9083,
    "extra": {
        "auth": "kerberos",
        "kerberos_principal": "hive/_HOST@EXAMPLE.COM",
        "kerberos_keytab": "/etc/security/hive.keytab"
    }
}
```

---

## 5. Troubleshooting

### Common Issues and Solutions

| Error | Likely Cause | Solution |
|-------|-------------|----------|
| `kinit: Pre-authentication failed` | Wrong password or expired principal | Verify credentials with admin |
| `kinit: Client not found` | Principal doesn't exist in KDC | Ask Kerberos admin to create it |
| `kinit: KDC reply did not match` | Clock skew > 5 minutes | Sync system clock via NTP |
| `javax.security.auth.login.Failed` | Keytab expired or wrong principal | Regenerate keytab; verify principal |
| `GSS initiate failed` | Missing or expired TGT | Run `kinit` before job |
| `SASL authentication error` | Wrong principal or keytab for Hive | Check Hive metastore principal name |
| `No valid credentials provided` | TGT expired or not obtained | Check `klist`; re-run `kinit` |

### Diagnostic Commands

```bash
# Check current tickets
klist

# Check for a specific principal
klist -5 -e

# Test authentication
kinit -V pipeline-svc@EXAMPLE.COM

# Test with keytab
kinit -kt /etc/security/pipeline.keytab -V

# Destroy all tickets
kdestroy

# Check keytab contents
klist -kt /etc/security/pipeline.keytab

# Test Hive connectivity
beeline -u "jdbc:hive2://hive-server:10000/default;principal=hive/_HOST@EXAMPLE.COM"

# Check clock skew
chronyc tracking  # or: ntpq -p
```

### Clock Synchronization

Kerberos requires clock skew to be under 5 minutes (configurable):

```bash
# Install NTP
sudo apt-get install -y ntp  # Debian
sudo yum install -y chrony    # RHEL

# Enable and start
sudo systemctl enable --now ntp    # or chronyd

# Verify
timedatectl
```

---

## 6. Security Best Practices

1. **Keytab Protection**
   - Store keytabs with `chmod 400` (owner read-only)
   - Use dedicated service principals (not user principals)
   - Rotate keytabs periodically per security policy
   - Use different keytabs for different services

2. **Ticket Management**
   - Renew tickets before expiry in long-running processes
   - Use `kinit -R` for ticket renewal when possible
   - Destroy tickets after use in shared environments

3. **Network Security**
   - Never transmit keytabs over unencrypted channels (use SCP/SFTP)
   - Kerberos traffic uses UDP/TCP port 88 — ensure firewall rules
   - KDC admin port (749) should be restricted to authorized hosts

4. **Monitoring**
   - Monitor keytab expiry dates
   - Alert on authentication failures
   - Log all `kinit` and `kdestroy` operations
   - Use `ktutil` to inspect keytab version numbers

5. **Principal Naming**
   - Use format: `service/hostname@REALM` for service principals
   - Use format: `username@REALM` for user principals
   - Avoid embedding environment names in principals (use `_HOST` placeholder)

---

## Quick Reference

```bash
# === Obtain ticket ===
kinit user@REALM
kinit -kt /path/to/keytab principal@REALM

# === List tickets ===
klist
klist -5 -e    # show encryption types

# === Check keytab ===
klist -kt /path/to/keytab

# === Destroy tickets ===
kdestroy

# === Test connection ===
beeline -u "jdbc:hive2://host:10000/;principal=hive/_HOST@REALM"

# === Spark submit with Kerberos ===
spark-submit --principal user@REALM --keytab /path/to/keytab job.py
```
