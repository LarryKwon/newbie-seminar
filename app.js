const express = require('express');
const app = express();
const port = 3000;
const {PrismaClient} = require('@prisma/client');
const prisma = new PrismaClient();

// For powershell: $env:DATABASE_URL="mysql://admin:tnfqkrtm@sparcs-mysql.cpghnwiewsdn.ap-northeast-2.rds.amazonaws.com:3306/bank"

app.get('/status', async (req, res) => { //sample endpoint
    res.status(200).json({ isOnline: true });
});

app.get('/sample', async (req, res) => { //sample endpoint
    const branches = await prisma.branch.findMany();
    return res.status(200).json(branches);
});

app.listen(port, () => {
    console.log('Starting Server...');
})