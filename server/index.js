const express = require('express');
const app = express();

const problemsRouter = require('./routes/problems');
const employeeRouter = require('./routes/employee');
const accountRouter = require('./routes/account');

app.use('/problems', problemsRouter);
app.use('/employee', employeeRouter);
app.use('/account', accountRouter);

app.get('/', (req, res) => {
    res.send(`Noah's Prisma Server!`);
})

app.listen(8080, () => {
    console.log('App is currently running...');
})