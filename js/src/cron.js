const DAYS = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
const DAYS_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const MONTHS = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'
]

function isCron(str) {
    if (!str || typeof str !== 'string') return false
    const parts = str.trim().split(/\s+/)
    return parts.length === 5 && parts.every((p) => /^[\d,\-\*\/A-Za-z]+$/.test(p))
}

function parseField(field, max, names = null) {
    if (field === '*') return null

    if (field.startsWith('*/')) {
        return { every: parseInt(field.slice(2), 10) }
    }

    if (field.includes('-') && !field.includes(',')) {
        const [start, end] = field.split('-')
        const startVal = names ? parseNamedValue(start, names) : parseInt(start, 10)
        const endVal = names ? parseNamedValue(end, names) : parseInt(end, 10)
        return { range: [startVal, endVal] }
    }

    if (field.includes(',')) {
        const values = field.split(',').map((v) => (names ? parseNamedValue(v, names) : parseInt(v, 10)))
        return { list: values }
    }

    const val = names ? parseNamedValue(field, names) : parseInt(field, 10)
    return { value: val }
}

function parseNamedValue(val, names) {
    const upper = val.toUpperCase()
    const idx = names.findIndex((n) => n.toUpperCase().startsWith(upper))
    return idx !== -1 ? idx : parseInt(val, 10)
}

function formatTime(hour, minute) {
    const h = hour % 12 || 12
    const ampm = hour < 12 ? 'AM' : 'PM'
    const m = minute.toString().padStart(2, '0')
    return m === '00' ? `${h} ${ampm}` : `${h}:${m} ${ampm}`
}

function formatOrdinal(n) {
    const s = ['th', 'st', 'nd', 'rd']
    const v = n % 100
    return n + (s[(v - 20) % 10] || s[v] || s[0])
}

export function cronToHuman(schedule) {
    if (!schedule) return ''
    if (!isCron(schedule)) return schedule

    const parts = schedule.trim().split(/\s+/)
    const [minute, hour, dayOfMonth, month, dayOfWeek] = parts

    const minParsed = parseField(minute, 59)
    const hourParsed = parseField(hour, 23)
    const domParsed = parseField(dayOfMonth, 31)
    const monthParsed = parseField(month, 12, MONTHS)
    const dowParsed = parseField(dayOfWeek, 6, DAYS_SHORT)

    let result = ''

    if (minParsed?.every && !hourParsed && !domParsed && !monthParsed && !dowParsed) {
        return `Every ${minParsed.every} minutes`
    }

    if (hourParsed?.every && (minute === '0' || minute === '*') && !domParsed && !monthParsed && !dowParsed) {
        return `Every ${hourParsed.every} hours`
    }

    let timeStr = ''
    if (hourParsed?.value !== undefined && minParsed?.value !== undefined) {
        timeStr = formatTime(hourParsed.value, minParsed.value)
    } else if (hourParsed?.value !== undefined && !minParsed) {
        timeStr = formatTime(hourParsed.value, 0)
    } else if (minParsed?.value !== undefined && minute !== '0') {
        timeStr = `:${minParsed.value.toString().padStart(2, '0')}`
    }

    if (dowParsed && !domParsed && !monthParsed) {
        if (dowParsed.range) {
            const [start, end] = dowParsed.range
            if (start === 1 && end === 5) {
                result = 'Weekdays'
            } else if (start === 0 && end === 6) {
                result = 'Daily'
            } else {
                result = `${DAYS[start]}-${DAYS[end]}`
            }
        } else if (dowParsed.list) {
            result = dowParsed.list.map((d) => DAYS_SHORT[d]).join(', ')
        } else if (dowParsed.value !== undefined) {
            result = DAYS[dowParsed.value] + 's'
        }
        if (timeStr) result += ` at ${timeStr}`
        return result
    }

    if (domParsed && !dowParsed) {
        if (domParsed.value !== undefined) {
            result = formatOrdinal(domParsed.value) + ' of '
            if (monthParsed?.value !== undefined) {
                result += MONTHS[monthParsed.value - 1]
            } else {
                result += 'each month'
            }
        }
        if (timeStr) result += ` at ${timeStr}`
        return result
    }

    if (!domParsed && !dowParsed && !monthParsed && timeStr) {
        return `Daily at ${timeStr}`
    }

    return schedule
}

export function formatSchedule(schedule) {
    return cronToHuman(schedule)
}
