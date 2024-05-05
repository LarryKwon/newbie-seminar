const express = require('express')
const app = express();
const port = 8080;

const problemsRouter = require('./prisma-query')

app.use('/problems', problemsRouter)

app.listen(port, () => {
    console.log(`Example App Listening @ http://localhost:${ port }`)
})
