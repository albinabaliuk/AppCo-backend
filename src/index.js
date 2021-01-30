const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')
const {
  connectDB,
  createTables,
  insertUsersIfITtIsEmpty,
  insertUserStatisticIfITtIsEmpty,
  paginateUserStatistics,
  getUserStatistic,
  getUserStatisticByDate
} = require('./db')

const app = express()
const port = 8888

app.use(bodyParser.json())
app.use(cors({
  "origin": "*",
  "methods": "GET,HEAD,PUT,PATCH,POST,DELETE",
  "preflightContinue": false,
  "optionsSuccessStatus": 204
}))

const db = connectDB()
createTables(db)
insertUsersIfITtIsEmpty(db)
insertUserStatisticIfITtIsEmpty(db)

app.get('/list.users.statistic', (req, res) => {
  try {
    const limit = Number(req.query.limit)
    const page = Number(req.query.page)

    if (typeof limit !== 'number' || typeof page !== 'number') {
      return res.status(400).end()
    }

    const params = {
      limit: limit,
      page: page
    }

    paginateUserStatistics(db, params, res)
  } catch (err) {
    console.error('Cannot paginate users statistic', err)
    res.status(500).end()
  }
})

app.get('/get.user.statistic', (req, res) => {
  try {
    if (typeof req.query.user_id !== 'number') {
      return res.status(400).end()
    }

    const params = {
      user_id: req.query.user_id
    }

    getUserStatistic(db, params, res)

  } catch (err) {
  console.error('Cannot get user statistic', err)
  res.status(500).end()
}
})

app.get('/get.user.statistic.by.date', (req, res) => {
  try {
    if (
      typeof req.body.user_id !== 'number' ||
      typeof req.body.start_date !== 'string' ||
      typeof req.body.end_date !== 'string'
    ) {
      return res.status(400).end()
    }

    const params = {
      user_id: req.body.user_id,
      start_date: req.body.start_date,
      end_date: req.body.end_date
    }

    getUserStatisticByDate(db, params, res)

  } catch (err) {
    console.error('Cannot get user statistic', err)
    res.status(500).end()
  }
})

app.listen(port, () => {
  console.log('Listening at', port)
})
