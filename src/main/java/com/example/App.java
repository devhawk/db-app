package com.example;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.List;
import java.util.Map;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

public class App {
    public static void main(String[] args) {
        // 1. Setup JDBI with PostgreSQL
        String host = System.getenv().getOrDefault("DB_HOST", "localhost:5432");
        String user = System.getenv().getOrDefault("PGUSER", "postgres");
        String password = System.getenv().getOrDefault("PGPASSWORD", "postgres");
        String dbName = System.getenv().getOrDefault("DB_NAME", "bank");
        
        // Ensure the database exists
        ensureDatabaseExists(host, user, password, dbName);
        
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://" + host + "/" + dbName);
        var jdbi = Jdbi.create(url, user, password);
        jdbi.installPlugin(new SqlObjectPlugin());
        
        // Register mapper for Account record
        jdbi.registerRowMapper(Account.class, ConstructorMapper.of(Account.class));

        // Initialize Database Schema
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS accounts (id SERIAL PRIMARY KEY, name VARCHAR(255), balance DOUBLE PRECISION)");
            handle.execute("CREATE TABLE IF NOT EXISTS transfer_log (id SERIAL PRIMARY KEY, from_account BIGINT, to_account BIGINT, amount DOUBLE PRECISION, status VARCHAR(50), error_message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            
            // Only seed if empty
            Integer count = handle.createQuery("SELECT COUNT(*) FROM accounts").mapTo(Integer.class).one();
            if (count == 0) {
                handle.execute("INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00)");
                handle.execute("INSERT INTO accounts (name, balance) VALUES ('Bob', 500.00)");
            }
        });

        // 2. Setup Javalin
        Javalin.create()
            .get("/", ctx -> ctx.result("Java DB-backed Web App (Javalin + JDBI)"))
            
            // List all accounts
            .get("/accounts", ctx -> {
                List<Account> accounts = jdbi.withHandle(handle -> getAllAccounts(handle));
                ctx.json(accounts);
            })

            // Transactional Transfer Operation
            .post("/transfer", ctx -> {
                var body = ctx.bodyAsClass(TransferRequest.class);
                String status = "SUCCESS";
                String errorMessage = null;
                
                try {
                    jdbi.useTransaction(handle -> {
                        deductFromAccount(handle, body.from(), body.amount());
                        addToAccount(handle, body.to(), body.amount());
                    });
                    
                    ctx.status(HttpStatus.OK).json(Map.of("message", "Transfer successful"));
                } catch (Exception e) {
                    status = "FAILED";
                    errorMessage = e.getMessage();
                    ctx.status(HttpStatus.BAD_REQUEST).json(Map.of("error", e.getMessage()));
                } finally {
                    final String finalStatus = status;
                    final String finalError = errorMessage;
                    // Log the transfer attempt outside the main transaction
                    jdbi.useHandle(handle -> 
                        logTransfer(handle, body.from(), body.to(), body.amount(), finalStatus, finalError)
                    );
                }
            })
            .start(7070);
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
    
    /**
     * Retrieves all accounts from the database.
     */
    private static List<Account> getAllAccounts(Handle handle) {
        return handle.createQuery("SELECT * FROM accounts")
            .mapTo(Account.class)
            .list();
    }
    
    /**
     * Deducts an amount from an account's balance.
     * Throws RuntimeException if insufficient funds or account not found.
     */
    private static void deductFromAccount(Handle handle, long accountId, double amount) {
        int updated = handle.createUpdate("UPDATE accounts SET balance = balance - :amount WHERE id = :id AND balance >= :amount")
            .bind("id", accountId)
            .bind("amount", amount)
            .execute();
        
        if (updated == 0) {
            throw new RuntimeException("Insufficient funds or sender not found");
        }
    }
    
    /**
     * Adds an amount to an account's balance.
     * Throws RuntimeException if account not found.
     */
    private static void addToAccount(Handle handle, long accountId, double amount) {
        int updated = handle.createUpdate("UPDATE accounts SET balance = balance + :amount WHERE id = :id")
            .bind("id", accountId)
            .bind("amount", amount)
            .execute();
        
        if (updated == 0) {
            throw new RuntimeException("Receiver not found");
        }
    }
    
    /**
     * Logs a transfer attempt to the transfer_log table.
     */
    private static void logTransfer(Handle handle, long fromAccount, long toAccount, double amount, String status, String errorMessage) {
        handle.createUpdate("INSERT INTO transfer_log (from_account, to_account, amount, status, error_message) VALUES (:from, :to, :amount, :status, :error)")
            .bind("from", fromAccount)
            .bind("to", toAccount)
            .bind("amount", amount)
            .bind("status", status)
            .bind("error", errorMessage)
            .execute();
    }

    // Helper class for JSON body
    public record TransferRequest(long from, long to, double amount) {}
}
