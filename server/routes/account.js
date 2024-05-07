const express = require('express');
const router = express.Router();

const {PrismaClient} = require('@prisma/client');
const { curry } = require('lodash');
const prisma = new PrismaClient();


router.post('/:account_no/deposit', async (req, res) => {
    try {
        const input = req.body;
        const inputAccountNumber = req.params.account_no;
        const OwnersID = await prisma.customer.findMany({
            where: {
                Owns: {
                    some: {
                        accNumber: inputAccountNumber,
                    }
                }
            },
            select: {
                customerID: true,
            }
        });
        let isHeOnwer = false;
        OwnersID.forEach((obj) => {
            if (obj.customerID === input["customerID"]) {
                isHeOnwer = true;
            }
        })
        if (isHeOnwer) {
            const objectCurrentBalance = await prisma.account.findFirst({
                where: {
                    accNumber: inputAccountNumber,
                },
                select: {
                    balance: true
                }
            });
            currentBalance = Number(objectCurrentBalance.balance);
            const transaction = await prisma.customer.update({
                where: {
                    accNumber:  inputAccountNumber,
                },
                update: {
                    balance: stringify(currentBalance + input["depositValue"]),
                },
            });
            res.status(200).json({success: true, "balance": currentBalance + input["depositValue"]});
        } else {
            res.status(403).json({success: false, "reason": "Not an Owner"})
        }
    } catch(e) {
        res.status(500).json({error: e});
    }
})

router.post('/:account_no/withdraw', async (req, res) => {
    try {
        const input = req.body;
        const inputAccountNumber = req.params.account_no;
        const OwnersID = await prisma.customer.findMany({
            where: {
                Owns: {
                    some: {
                        accNumber: inputAccountNumber,
                    }
                }
            },
            select: {
                customerID: true,
            }
        });
        let isHeOnwer = false;
        OwnersID.forEach((obj) => {
            if (obj.customerID === input["customerID"]) {
                isHeOnwer = true;
            }
        })
        if (isHeOnwer) {
            const objectCurrentBalance = await prisma.account.findFirst({
                where: {
                    accNumber: inputAccountNumber,
                },
                select: {
                    balance: true
                }
            });
            currentBalance = Number(objectCurrentBalance.balance);
            if (currentBalance - input["withdrawValue"] >= 0) {
                const transaction = await prisma.customer.update({
                    where: {
                        accNumber:  inputAccountNumber,
                    },
                    update: {
                        balance: stringify(currentBalance - input["withdrawValue"]),
                    },
                });
                res.status(200).json({success: true, "balance": currentBalance - input["withdrawValue"]});
            } else {
                res.status(403).json({success: false, "reason": "Out of balance"});
            }
            
        } else {
            res.status(403).json({success: false, "reason": "Not an Owner"})
        }
    } catch(e) {
        res.status(500).json({error: e});
    }
})

module.exports = router;