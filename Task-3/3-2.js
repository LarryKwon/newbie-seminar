const express = require('express');
const router = express.Router();
const { PrismaClient } = require('@prisma/client');
const prisma = new PrismaClient();

router.use(express.json());

router.post("/employee/join", async(req, res) => {
    const { sin, firstName, salary, branchNumber } = req.body;

    try {
        const newEmployee = await prisma.employee.create({
            data: {
                sin,
                firstName,
                lastName,
                salary,
                branchNumber
            }
        });
        res.status(200).send(`이 팀은 미친듯이 일하는 일꾼들로 이루어진 광전사 설탕 노움 조합이다.
         분위기에 적응하기는 쉽지 않지만 아주 화력이 좋은 강력한 조합인거 같다.`);
    } catch (error) {
        res.status(500).send(error)
    }
});

router.delete("/employee/leave", async (req, res) => {
    const { sin } = req.body;
    if (!sin) {
        return res.status(400).send("SIN 번호가 필요합니다.");
    }

    const sinNumber = parseInt(sin, 10);
    if (isNaN(sinNumber)) {
        return res.status(400).send("잘못된 SIN 번호입니다.");
    }

    const employee = await prisma.employee.findUnique({ where: { sin: sinNumber } });
    if (!employee) {
        return res.status(404).send("직원을 찾을 수 없습니다.");
    }

    await prisma.employee.delete({ where: { sin: sinNumber } });
    res.status(200).send("안녕히 계세요 여러분!\n전 이 세상의 모든 굴레와 속박을 벗어 던지고 제 행복을 찾아 떠납니다!\n여러분도 행복하세요~~!");
});

router.post("/account/:account_no/deposit", async (req, res) => {
    const { account_no } = req.params;
    const { customerID, amount } = req.body;
  
    const accNumber = parseInt(account_no);
    if (isNaN(accNumber) || amount <= 0) {
        return res.status(400).json({ error: "Invalid account number or amount" });
    }
  
    try {
        const account = await prisma.account.findUnique({
            where: { accNumber },
            include: { owner: true }
        });
    
        if (!account) {
            return res.status(404).json({ error: "Account not found" });
        }
    
        if (account.owner.customerID !== customerID) {
            return res.status(403).json({ error: "Access denied: You do not own this account" });
        }
    
        const newBalance = parseFloat(account.balance) + parseFloat(amount);
        const updatedAccount = await prisma.account.update({
            where: { accNumber },
            data: { balance: newBalance.toString() }
        });
    
        res.status(200).json({ balance: updatedAccount.balance });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Internal server error" });
    }
});

router.post("/account/:account_no/withdraw", async (req, res) => {
    const { account_no } = req.params;
    const { customerID, amount } = req.body;
  
    const accNumber = parseInt(account_no);
    if (isNaN(accNumber) || amount <= 0) {
        return res.status(400).json({ error: "Invalid account number or amount" });
    }
  
    try {
        const account = await prisma.account.findUnique({
            where: { accNumber },
            include: { owner: true }
        });
    
        if (!account) {
            return res.status(404).json({ error: "Account not found" });
        }
    
        const currentBalance = parseFloat(account.balance);
        if (account.owner.customerID !== customerID) {
            return res.status(403).json({ error: "Access denied: You do not own this account" });
        }
    
        if (currentBalance < amount) {
            return res.status(400).json({ error: "Insufficient balance" });
        }
    
        const newBalance = currentBalance - amount;
        const updatedAccount = await prisma.account.update({
            where: { accNumber },
            data: { balance: newBalance.toString() }
        });
    
        res.status(200).json({ balance: updatedAccount.balance });
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Internal server error" });
    }
});

module.exports = router;
