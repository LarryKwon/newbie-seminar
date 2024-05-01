const express = require('express');
const app = express();
const port = 3000;
const {PrismaClient} = require('@prisma/client');
const prisma = new PrismaClient();
const axios = require('axios');
const fs = require('node:fs');
const lodash = require('lodash');

// For powershell: $env:DATABASE_URL="mysql://admin:tnfqkrtm@sparcs-mysql.cpghnwiewsdn.ap-northeast-2.rds.amazonaws.com:3306/teddybear"

app.get('/status', async (req, res) => {
    res.status(200).json({ isOnline: true });
});

const ProblemList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 15, 17, 18];

app.get('/checker', async (req, res) => {
    rtval = {};
    for (let i = 0; i < ProblemList.length; i++) {
        try {
            const response = await axios.get(`http://localhost:3000/problems/${ProblemList[i]}`);
            const result = response.data;
            const answer = JSON.parse(fs.readFileSync(`Problem${ProblemList[i]}.json`,'utf-8'));
            lodash.isEqual(result, answer) ? rtval[ProblemList[i]] = 'correct' : rtval[ProblemList[i]] = 'incorrect';
        } catch (error) {
            if (error.response) {
                if (error.response.status == 404) {
                    rtval[ProblemList[i]] = 'notimplemented';
                } else {
                    console.log(error.response.status);
                    rtval[ProblemList[i]] = 'error';
                }
            } else {
                console.log(error);
                rtval[ProblemList[i]] = 'error';
            }
        }
    }
    return res.status(200).json(rtval);
});

app.get('/problems/0', async (req, res) => { //sample endpoint
    try {
        const result = await prisma.branch.findMany();
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.get('/problems/1', async (req, res) => {
    try {
        const result = await prisma.customer.findMany({
            where: {
                AND: {
                    income: { gt: 50000, },
                    income: { lt: 60000, },
                }
            },
            orderBy: [
                { income: 'desc', },
                { lastName: 'asc', },
                { firstName: 'asc', },
            ],
            select: {
                firstName: true,
                lastName: true,
                income: true
            },
            take: 10
        });
        return res.status(200).json(result);
    } catch (e) {
        console.log(e);
        return res.status(500).json({});
    }
});

app.listen(port, () => {
    console.log('Starting Server...');
});