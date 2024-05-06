const express = require('express');
const app = express();

const problemsRouter = require('./routes/problems');
app.use('/problems', problemsRouter);

app.get('/', (req, res) => {
    res.send(`Noah's Prisma Server!`);
})

app.listen(8080, () => {
    console.log('App is currently running...');
})