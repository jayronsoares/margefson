CREATE DATABASE IF NOT EXISTS payments_db;

USE payments_db;

CREATE TABLE IF NOT EXISTS payments_db.payments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    situacao VARCHAR(20),
    irmao VARCHAR(100),
    vencimento DATE,
    valor DECIMAL(10, 2),
    descricao TEXT,
    tipo VARCHAR(50),
    forma_pagamento VARCHAR(50)
);
