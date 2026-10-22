package io.github.p2pact

/**
 * A small JSON reader and writer for the device files (net-key.json, net-peers.json) and the
 * interop peer. Values: Map<String, Any?>, List<Any?>, String, Double/Long, Boolean, null.
 */
object Json {
    fun parse(s: String): Any? {
        val p = Parser(s)
        p.ws()
        val v = p.value(0)
        p.ws()
        if (p.i != s.length) throw IllegalArgumentException("json: trailing data")
        return v
    }

    fun write(v: Any?, indent: String? = null): String = StringBuilder().also { write(it, v, indent, "") }.toString()

    private fun write(sb: StringBuilder, v: Any?, indent: String?, cur: String) {
        when (v) {
            null -> sb.append("null")
            is Boolean -> sb.append(v)
            is Int, is Long, is Short, is Byte -> sb.append(v)
            is Number -> {
                val d = v.toDouble()
                if (d == Math.floor(d) && Math.abs(d) < 1e15) sb.append(d.toLong()) else sb.append(d)
            }
            is String -> quote(sb, v)
            is Map<*, *> -> {
                if (v.isEmpty()) { sb.append("{}"); return }
                sb.append('{')
                val next = if (indent != null) cur + indent else cur
                var first = true
                for ((k, x) in v) {
                    if (!first) sb.append(',')
                    first = false
                    if (indent != null) sb.append('\n').append(next)
                    quote(sb, k.toString())
                    sb.append(if (indent != null) ": " else ":")
                    write(sb, x, indent, next)
                }
                if (indent != null) sb.append('\n').append(cur)
                sb.append('}')
            }
            is List<*> -> {
                if (v.isEmpty()) { sb.append("[]"); return }
                sb.append('[')
                val next = if (indent != null) cur + indent else cur
                v.forEachIndexed { i, x ->
                    if (i > 0) sb.append(',')
                    if (indent != null) sb.append('\n').append(next)
                    write(sb, x, indent, next)
                }
                if (indent != null) sb.append('\n').append(cur)
                sb.append(']')
            }
            else -> quote(sb, v.toString())
        }
    }

    private fun quote(sb: StringBuilder, s: String) {
        sb.append('"')
        for (c in s) {
            when (c) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> if (c < ' ') sb.append(String.format("\\u%04x", c.code)) else sb.append(c)
            }
        }
        sb.append('"')
    }

    private class Parser(val s: String) {
        var i = 0

        fun ws() {
            while (i < s.length && s[i].isWhitespace()) i++
        }

        fun fail(): Nothing = throw IllegalArgumentException("json: bad input at $i")

        fun value(depth: Int): Any? {
            if (depth > 64 || i >= s.length) fail()
            return when (val c = s[i]) {
                '{' -> {
                    i++
                    val m = LinkedHashMap<String, Any?>()
                    ws()
                    if (i < s.length && s[i] == '}') { i++; return m }
                    while (true) {
                        ws()
                        if (i >= s.length || s[i] != '"') fail()
                        val k = string()
                        ws()
                        if (i >= s.length || s[i] != ':') fail()
                        i++
                        ws()
                        m[k] = value(depth + 1)
                        ws()
                        if (i >= s.length) fail()
                        if (s[i] == ',') { i++; continue }
                        if (s[i] == '}') { i++; return m }
                        fail()
                    }
                    @Suppress("UNREACHABLE_CODE") m
                }
                '[' -> {
                    i++
                    val l = ArrayList<Any?>()
                    ws()
                    if (i < s.length && s[i] == ']') { i++; return l }
                    while (true) {
                        ws()
                        l.add(value(depth + 1))
                        ws()
                        if (i >= s.length) fail()
                        if (s[i] == ',') { i++; continue }
                        if (s[i] == ']') { i++; return l }
                        fail()
                    }
                    @Suppress("UNREACHABLE_CODE") l
                }
                '"' -> string()
                't' -> word("true", true)
                'f' -> word("false", false)
                'n' -> word("null", null)
                else -> if (c == '-' || c in '0'..'9') number() else fail()
            }
        }

        fun word(w: String, v: Any?): Any? {
            if (!s.startsWith(w, i)) fail()
            i += w.length
            return v
        }

        fun number(): Any {
            val start = i
            while (i < s.length && (s[i] in "+-.eE" || s[i] in '0'..'9')) i++
            val t = s.substring(start, i)
            return t.toLongOrNull() ?: t.toDoubleOrNull() ?: fail()
        }

        fun string(): String {
            i++ // the opening quote
            val sb = StringBuilder()
            while (true) {
                if (i >= s.length) fail()
                val c = s[i++]
                when (c) {
                    '"' -> return sb.toString()
                    '\\' -> {
                        if (i >= s.length) fail()
                        when (val e = s[i++]) {
                            'n' -> sb.append('\n')
                            't' -> sb.append('\t')
                            'r' -> sb.append('\r')
                            'b' -> sb.append('\b')
                            'f' -> sb.append('\u000c')
                            'u' -> {
                                if (i + 4 > s.length) fail()
                                sb.append(s.substring(i, i + 4).toInt(16).toChar())
                                i += 4
                            }
                            else -> sb.append(e)
                        }
                    }
                    else -> sb.append(c)
                }
            }
        }
    }
}
