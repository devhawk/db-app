package com.example;

import io.javalin.Javalin;
import io.javalin.http.HttpStatus;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.List;
import java.util.Map;

public class App {
    public static void main(String[] args) {
        // 1. Setup JDBI with PostgreSQL
        String url = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://localhost:5432/bank");
        String user = System.getenv().getOrDefault("PGUSER", "postgres");
        String password = System.getenv().getOrDefault("PGPASSWORD", "postgres");
        
        var jdbi = Jdbi.create(url, user, password);
        jdbi.installPlugin(new SqlObjectPlugin());

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
        var app = Javalin.create()
            .get("/", ctx -> ctx.result("Java DB-backed Web App (Javalin + JDBI)"))
            
            // List all accounts
            .get("/accounts", ctx -> {
                List<Account> accounts = jdbi.withHandle(handle ->
                    handle.createQuery("SELECT * FROM accounts")
                        .mapTo(Account.class)
                        .list()
                );
                ctx.json(accounts);
            })

            // Transactional Transfer Operation
            .post("/transfer", ctx -> {
                var body = ctx.bodyAsClass(TransferRequest.class);
                String status = "SUCCESS";
                String errorMessage = null;
                
                try {
                    jdbi.useTransaction(handle -> {
                        // 1. Deduct from sender
                        int updatedFrom = handle.createUpdate("UPDATE accounts SET balance = balance - :amount WHERE id = :id AND balance >= :amount")
                            .bind("id", body.from())
                            .bind("amount", body.amount())
                            .execute();

                        if (updatedFrom == 0) {
                            throw new RuntimeException("Insufficient funds or sender not found");
                        }

                        // 2. Add to receiver
                        int updatedTo = handle.createUpdate("UPDATE accounts SET balance = balance + :amount WHERE id = :id")
                            .bind("id", body.to())
                            .bind("amount", body.amount())
                            .execute();

                        if (updatedTo == 0) {
                            throw new RuntimeException("Receiver not found");
                        }
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
                        handle.createUpdate("INSERT INTO transfer_log (from_account, to_account, amount, status, error_message) VALUES (:from, :to, :amount, :status, :error)")
                            .bind("from", body.from())
                            .bind("to", body.to())
                            .bind("amount", body.amount())
                            .bind("status", finalStatus)
                            .bind("error", finalError)
                            .execute()
                    );
                }
            })
            .start(7070);
    }

    // Helper class for JSON body
    public record TransferRequest(long from, long to, double amount) {}
}
