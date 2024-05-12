const express = require('express')
const app = express();
const port = 8080;

const problemsRouter = require('./Task-3/3-1')

const task2Router = require('./Task-3/3-2');
app.use(task2Router);

app.use('/problems', problemsRouter)

app.listen(port, () => {
    console.log(`Example App Listening @ http://localhost:${ port }`)
})

