# Javalin DB-Backed Demo

A simple Java web application using Javalin and JDBI with an H2 in-memory database.

## Prerequisites
- Java 21+
- Gradle (included in environment)

## How to Run
```bash
gradle run
```

## Endpoints

### 1. Welcome
```bash
curl http://localhost:7070/
```

### 2. List Accounts
```bash
curl http://localhost:7070/accounts
```

### 3. Transfer Money (Transactional)
This operation is transactional. If either the sender has insufficient funds or the receiver doesn't exist, the entire operation is rolled back.

**Successful Transfer:**
```bash
curl -X POST http://localhost:7070/transfer 
     -H "Content-Type: application/json" 
     -d '{"from": 1, "to": 2, "amount": 100.0}'
```

**Failed Transfer (Insufficient Funds):**
```bash
curl -X POST http://localhost:7070/transfer 
     -H "Content-Type: application/json" 
     -d '{"from": 1, "to": 2, "amount": 10000.0}'
```

## Tech Stack
- **Javalin**: Web framework.
- **JDBI**: Declarative and imperative database access.
- **H2**: In-memory SQL database.
- **Jackson**: JSON processing.
