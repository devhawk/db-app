package com.example;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

record TxOutput(String output, String error) {

    @SuppressWarnings("unchecked")
    <T, X extends Exception> T getReturnValue() throws X {
        if (error != null) {
            var throwable = JSONUtil.deserializeAppException(error);
            if (!(throwable instanceof Exception)) {
                throw new RuntimeException(throwable.getMessage(), throwable);
            } else {
                throw (X) throwable;
            }
        }

        if (output != null) {
            var array = JSONUtil.deserializeToArray(output);
            return array == null ? null : (T) array[0];
        }

        return null;
    }
}

// The helper methods in this class will eventually be a part of DBOS.
// They are not required, but this is boilerplate for tx step providers that use
// PG.

class DBOSHelpers {
    public static void createTxOutputsTable(Connection connection, String schema) throws SQLException {
        var sanitizedSchema = SystemDatabase.sanitizeSchema(schema);

        // Create schema if it doesn't exist
        String createSchemaSQL = "CREATE SCHEMA IF NOT EXISTS %s".formatted(sanitizedSchema);
        try (PreparedStatement stmt = connection.prepareStatement(createSchemaSQL)) {
            stmt.executeUpdate();
        }

        // Create table in the specified schema
        String sql = """
                CREATE TABLE IF NOT EXISTS %s.tx_outputs (
                    workflow_uuid TEXT NOT NULL,
                    function_id INTEGER NOT NULL,
                    output TEXT,
                    error TEXT,
                    PRIMARY KEY (workflow_uuid, function_id)
                )""".formatted(sanitizedSchema);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.executeUpdate();
        }
    }

    public static void saveTxResult(Connection connection, String schema, String workflowUuid, int functionId,
            String output, String error) throws SQLException {
        String sanitizedSchema = SystemDatabase.sanitizeSchema(schema);
        String sql = "INSERT INTO %s.tx_outputs (workflow_uuid, function_id, output, error) VALUES (?, ?, ?, ?)"
                .formatted(sanitizedSchema);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, workflowUuid);
            stmt.setInt(2, functionId);
            stmt.setString(3, output);
            stmt.setString(4, error);
            stmt.executeUpdate();
        }
    }

    public static TxOutput getTxResult(Connection connection, String schema, String workflowUuid, int functionId)
            throws SQLException {
        String sanitizedSchema = SystemDatabase.sanitizeSchema(schema);
        String sql = "SELECT output, error FROM %s.tx_outputs WHERE workflow_uuid = ? AND function_id = ?"
                .formatted(sanitizedSchema);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, workflowUuid);
            stmt.setInt(2, functionId);
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return new TxOutput(
                            rs.getString("output"),
                            rs.getString("error"));
                }
                return null;
            }
        }
    }
}

// This is a sample step provider that uses JDBI.
// You can imagine similar providers for raw JDBC, JOOQ, Spring Transactions,
// etc.
// need to look at using DBOS lifetime to handle creating tx outputs table

class JdbiTxStepProvider {

    final Jdbi jdbi;
    final String schema;

    public JdbiTxStepProvider(Jdbi jdbi) {
        this(jdbi, null);
    }

    public JdbiTxStepProvider(Jdbi jdbi, String schema) {
        this.jdbi = jdbi;
        this.schema = schema;
    }

    // these private helpers adapt the JDBI api to the DBOSHelpers raw JDBC api
    private void saveTxResult(String workflowUuid, int functionId, String output, String error) {
        jdbi.useHandle(handle -> saveTxResult(handle, workflowUuid, functionId, output, error));
    }

    private void saveTxResult(Handle handle, String workflowUuid, int functionId, String output, String error) {
        try {
            DBOSHelpers.saveTxResult(handle.getConnection(), schema, workflowUuid, functionId, output, error);
        } catch (SQLException e) {
            throw new UnableToExecuteStatementException(e, null);
        }
    }

    private TxOutput getTxResult(String workflowUuid, int functionId) {
        return jdbi.withHandle(handle -> {
            try {
                return DBOSHelpers.getTxResult(handle.getConnection(), schema, workflowUuid, functionId);
            } catch (SQLException e) {
                throw new UnableToExecuteStatementException(e, null);
            }
        });
    }

    public static void createTxOutputsTable(Jdbi jdbi, String schema) {
        jdbi.useHandle(handle -> {
            try {
                DBOSHelpers.createTxOutputsTable(handle.getConnection(), schema);
            } catch (SQLException e) {
                throw new UnableToExecuteStatementException(e, null);
            }
        });
    }

    // this is the main runTxStep implementation, with additional overloads for
    // better developer experience when using default isolation level and/or not
    // having a return value.
    public <T, X extends Exception> T runTxStep(TransactionIsolationLevel isolationLevel, HandleCallback<T, X> callback,
            String name) throws X {
        return DBOS.runStep(() -> {

            // if we're not in a workflow, just run the callback in a transaction w/o DBOS
            // bookkeeping
            if (DBOS.inWorkflow() == false) {
                return jdbi.inTransaction(isolationLevel, callback);
            }

            final var wfid = DBOS.workflowId();
            final var stepid = DBOS.stepId();

            // check to see if the step has a transactionally persisited output.
            // return it if the output already exists.
            var result = getTxResult(wfid, stepid);
            if (result != null) {
                return result.getReturnValue();
            }

            try {
                return jdbi.inTransaction(isolationLevel, h -> {
                    var returnVal = callback.withHandle(h);
                    var output = JSONUtil.serialize(returnVal);
                    saveTxResult(h, wfid, stepid, output, null);
                    return returnVal;
                });
            } catch (Exception e) {
                var error = JSONUtil.serializeAppException(e);
                saveTxResult(wfid, stepid, null, error);
                throw e;
            }
        }, name);
    }

    public <T, X extends Exception> T runTxStep(HandleCallback<T, X> callback, String name) throws X {
        return runTxStep(TransactionIsolationLevel.READ_COMMITTED, callback, name);
    }

    public <X extends Exception> void runTxStep(TransactionIsolationLevel isolationLevel, HandleConsumer<X> consumer,
            String name) throws X {
        runTxStep(isolationLevel, consumer.asCallback(), name);
    }

    public <X extends Exception> void runTxStep(HandleConsumer<X> consumer, String name) throws X {
        runTxStep(consumer.asCallback(), name);
    }
}

interface Bank {
    List<Account> getAllAccounts();

    void transfer(long from, long to, double amount);
}

class BankImpl implements Bank {

    final JdbiTxStepProvider stepProvider;

    public BankImpl(Jdbi jdbi) {
        this(new JdbiTxStepProvider(jdbi));
    }

    public BankImpl(JdbiTxStepProvider stepProvider) {
        this.stepProvider = stepProvider;
    }

    private List<Account> getAllAccounts(Handle handle) {
        return handle.createQuery("SELECT * FROM accounts").mapTo(Account.class).list();
    }

    private void transferFunds(Handle handle, long fromAccountId, long toAccountId, double amount) {
        // 1. Deduct from sender account (with balance validation)
        int deductResult = handle
                .createUpdate(
                        "UPDATE accounts SET balance = balance - :amount WHERE id = :id AND balance >= :amount")
                .bind("id", fromAccountId)
                .bind("amount", amount)
                .execute();

        if (deductResult == 0) {
            throw new RuntimeException("Insufficient funds or sender account not found");
        }

        // 2. Add to receiver account
        int addResult = handle.createUpdate("UPDATE accounts SET balance = balance + :amount WHERE id = :id")
                .bind("id", toAccountId)
                .bind("amount", amount)
                .execute();

        if (addResult == 0) {
            throw new RuntimeException("Receiver account not found");
        }
    }

    private void logTransfer(Handle handle, long fromAccount, long toAccount, double amount, String status,
            String errorMessage) {
        handle.createUpdate(
                "INSERT INTO transfer_log (from_account, to_account, amount, status, error_message) VALUES (:from, :to, :amount, :status, :error)")
                .bind("from", fromAccount)
                .bind("to", toAccount)
                .bind("amount", amount)
                .bind("status", status)
                .bind("error", errorMessage)
                .execute();
    }

    @Override
    public List<Account> getAllAccounts() {
        return stepProvider.runTxStep(h -> {
            return getAllAccounts(h);
        }, "getAllAccounts");
    }

    @Override
    @Workflow
    public void transfer(long from, long to, double amount) {
        String status = "SUCCESS";
        String errorMessage = null;
        try {
            stepProvider.runTxStep(h -> {
                transferFunds(h, from, to, amount);
            }, "transferFunds");
        } catch (Throwable e) {
            status = "FAILED";
            errorMessage = e.getMessage();
            throw e;
        } finally {
            final var _status = status;
            final var _error = errorMessage;
            stepProvider.runTxStep(h -> {
                logTransfer(h, from, to, amount, _status, _error);
            }, "logTransfer");
        }
    }
}

public class App {
    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("DB_HOST", "localhost:5432");
        String dbName = System.getenv().getOrDefault("DB_NAME", "bank_java");
        String user = System.getenv().getOrDefault("PGUSER", "postgres");
        String password = System.getenv().getOrDefault("PGPASSWORD", "postgres");

        ensureDatabaseExists(host, user, password, dbName);

        var url = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://" + host + "/" + dbName);
        var jdbi = Jdbi.create(url, user, password);
        jdbi.installPlugin(new SqlObjectPlugin());
        jdbi.registerRowMapper(Account.class, ConstructorMapper.of(Account.class));

        initializeDatabaseSchema(jdbi);
        JdbiTxStepProvider.createTxOutputsTable(jdbi, null);

        var dbosConfig = DBOSConfig.defaults("java-bank")
                .withDatabaseUrl(url)
                .withDbUser(user)
                .withDbPassword(password);
        DBOS.configure(dbosConfig);

        var bankImpl = new BankImpl(jdbi);
        var proxy = DBOS.registerWorkflows(Bank.class, bankImpl);

        Javalin.create()
                .events(event -> {
                    event.serverStarting(() -> DBOS.launch());
                    event.serverStopping(() -> DBOS.shutdown());
                })
                .get("/", ctx -> ctx.result("Java DB-backed Web App (Javalin + JDBI)"))
                .get("/accounts", ctx -> {
                    var accounts = bankImpl.getAllAccounts();
                    ctx.json(accounts);
                })

                // Transactional Transfer Operation
                .post("/transfer", ctx -> {
                    record TransferRequest(long from, long to, double amount) {
                    }
                    var body = ctx.bodyAsClass(TransferRequest.class);
                    try {
                        proxy.transfer(body.from, body.to, body.amount);
                        ctx.status(HttpStatus.OK).json(Map.of("message", "Transfer successful"));
                    } catch (Throwable e) {
                        ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", e.getMessage()));
                    }
                })
                .start(7070);
    }

    /**
     * Inserts an account with the given name and balance, ignoring duplicates.
     */
    private static void insertAccount(org.jdbi.v3.core.Handle handle, String name, double balance) {
        handle.createUpdate(
                "INSERT INTO accounts (name, balance) VALUES (:name, :balance) ON CONFLICT (name) DO NOTHING")
                .bind("name", name)
                .bind("balance", balance)
                .execute();
    }

    /**
     * Initializes the database schema by creating tables and seeding initial data.
     */
    private static void initializeDatabaseSchema(Jdbi jdbi) {
        jdbi.useHandle(handle -> {

            // Create tables if they don't exist
            handle.execute(
                    "CREATE TABLE IF NOT EXISTS accounts (id SERIAL PRIMARY KEY, name VARCHAR(255) UNIQUE, balance DOUBLE PRECISION)");
            handle.execute(
                    "CREATE TABLE IF NOT EXISTS transfer_log (id SERIAL PRIMARY KEY, from_account BIGINT, to_account BIGINT, amount DOUBLE PRECISION, status VARCHAR(50), error_message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            // Seed demo accounts (ignore duplicates)
            System.out.println("Seeding demo account data...");
            insertAccount(handle, "Alice", 1000.00);
            insertAccount(handle, "Bob", 500.00);
            insertAccount(handle, "Charlie", 750.00);
            insertAccount(handle, "Diana", 1250.00);
            insertAccount(handle, "Eve", 300.00);
            insertAccount(handle, "Frank", 2000.00);
            System.out.println("Demo accounts seeded successfully");
        });
    }

    /**
     * Ensures the specified database exists by connecting to the PostgreSQL server
     * and creating it if necessary.
     */
    private static void ensureDatabaseExists(String host, String user, String password, String dbName) {
        String adminUrl = "jdbc:postgresql://" + host + "/postgres";
        var adminJdbi = Jdbi.create(adminUrl, user, password);

        adminJdbi.useHandle(handle -> {
            try {
                // Check if database exists
                Integer count = handle.createQuery("SELECT 1 FROM pg_database WHERE datname = ?")
                        .bind(0, dbName)
                        .mapTo(Integer.class)
                        .findOne()
                        .orElse(0);

                if (count == 0) {
                    // Database doesn't exist, create it
                    System.out.println("Creating database: " + dbName);
                    handle.execute("CREATE DATABASE " + dbName);
                    System.out.println("Database " + dbName + " created successfully");
                } else {
                    System.out.println("Database " + dbName + " already exists");
                }
            } catch (UnableToExecuteStatementException e) {
                System.err.println("Failed to create database " + dbName + ": " + e.getMessage());
                throw e;
            }
        });
    }
}
