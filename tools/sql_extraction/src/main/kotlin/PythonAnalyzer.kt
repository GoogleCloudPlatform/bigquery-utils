package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.antlr.Python3BaseVisitor
import com.google.cloud.sqlecosystem.sqlextraction.antlr.Python3Parser
import com.google.cloud.sqlecosystem.sqlextraction.output.Location
import org.antlr.v4.runtime.Token

class PythonAnalyzer(private val dataFlowEngine: DataFlowEngine) : Python3BaseVisitor<Unit>() {
    override fun visitDecorator(ctx: Python3Parser.DecoratorContext?) {
        ctx!!.dotted_name().accept(this)

        if (ctx.arglist() != null) {
            dataFlowEngine.visitAnnotation(
                Location.combine(
                    ctx.OPEN_PAREN().symbol.getLocation(),
                    ctx.CLOSE_PAREN().symbol.getLocation()
                )
            ) { ctx.arglist().accept(this) }
        }
    }

    override fun visitFuncdef(ctx: Python3Parser.FuncdefContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    override fun visitParameters(ctx: Python3Parser.ParametersContext?) {
        if (ctx!!.typedargslist() != null) {
            if (!ctx.typedargslist().tfpdef().isNullOrEmpty()) {
                for (tfpdef in ctx.typedargslist().tfpdef()) {
                    dataFlowEngine.addMethodParameter(tfpdef.NAME().text!!)
                }
            }
        }
    }

    override fun visitStmt(ctx: Python3Parser.StmtContext?) =
        dataFlowEngine.visitStatement { visitChildren(ctx) }

    override fun visitReturn_stmt(ctx: Python3Parser.Return_stmtContext?) =
        dataFlowEngine.visitReturn(ctx!!.RETURN().symbol.getLocation()) { visitChildren(ctx) }

    override fun visitTrailer(ctx: Python3Parser.TrailerContext?) {
        if (ctx!!.OPEN_PAREN() != null) {
            // method call
            dataFlowEngine.visitMethodArguments(
                Location.combine(
                    ctx.OPEN_PAREN().symbol.getLocation(),
                    ctx.CLOSE_PAREN().symbol.getLocation()
                ),
                ctx.arglist()?.argument()?.asSequence()?.map { arg -> { arg.accept(this) } }
                    ?: emptySequence()
            )
        } else {
            visitChildren(ctx)
        }
    }

    override fun visitExpr_stmt(ctx: Python3Parser.Expr_stmtContext?) {
        if (ctx!!.annassign() != null) {
            // typed assignment
            val varName = ctx.testlist_star_expr()[0].text
            dataFlowEngine.declareVariable(varName)
            if (ctx.annassign().ASSIGN() != null) {
                dataFlowEngine.visitAssignment(varName, { ctx.annassign().test(1).accept(this) })
            }
        } else if (ctx.augassign() != null) {
            // +=
            dataFlowEngine.visitAssignment(
                ctx.testlist_star_expr()[0].text,
                { ctx.getChild(2).accept(this) },
                true
            )
        } else if (!ctx.ASSIGN().isNullOrEmpty()) {
            // = (potentially multiple assigns)
            var isFirst = true
            for (assignIndex in (ctx.childCount - 2) downTo 1 step 2) {
                dataFlowEngine.visitAssignmentWithOptionalDeclaration(
                    ctx.getChild(assignIndex - 1).text
                ) {
                    if (isFirst) {
                        ctx.getChild(assignIndex + 1).accept(this)
                        isFirst = false
                    }
                }
            }
        } else {
            visitChildren(ctx)
        }
    }

    override fun visitArith_expr(ctx: Python3Parser.Arith_exprContext?) {
        if (ctx!!.childCount > 1 && ctx.MINUS().isNullOrEmpty()) {
            // all operators are additions
            dataFlowEngine.visitConcatenation(
                ctx.term().asSequence().map { term -> { term.accept(this) } })
        } else {
            visitChildren(ctx)
        }
    }

    override fun visitAtom(ctx: Python3Parser.AtomContext?) {
        if (!ctx!!.STRING().isNullOrEmpty()) {
            if (ctx.STRING().size <= 1) {
                val literal = ctx.STRING()[0].text
                val location = ctx.STRING()[0].symbol.getLocation()
                val pair = getStringLiteralValue(literal, location)
                dataFlowEngine.visitStringLiteral(pair.first, pair.second)
            } else {
                dataFlowEngine.visitConcatenation(ctx.STRING().asSequence().map { str ->
                    {
                        val literal = str.text
                        val location = str.symbol.getLocation()
                        val pair = getStringLiteralValue(literal, location)
                        dataFlowEngine.visitStringLiteral(pair.first, pair.second)
                    }
                })
            }
        } else if (ctx.NAME() != null) {
            dataFlowEngine.visitVariable(ctx.NAME().text)
        } else {
            visitChildren(ctx)
        }
    }

    // region private utils

    /**
     * Extracts the literal String value within quotes.
     *
     * @param[stringToken] String with quotes.
     * @param[tokenLocation] Location including quotes.
     */
    private fun getStringLiteralValue(
        stringToken: String,
        tokenLocation: Location
    ): Pair<String, Location> {

        var startIndex = stringToken.indexOfAny(listOf("'''", "\"\"\""))
        val end = if (startIndex >= 0) {
            if (stringToken[startIndex] == '"') "\"\"\"" else "'''"
        } else {
            startIndex = stringToken.indexOfAny(charArrayOf('\'', '"'))
            if (startIndex < 0) {
                throw IllegalArgumentException("Unknown string format: $stringToken.")
            }
            if (stringToken[startIndex] == '"') "\"" else "'"
        }
        val endIndex = stringToken.lastIndexOf(end)
        val endDelta = stringToken.length - endIndex

        val strValue = stringToken.substring(startIndex + end.length, endIndex)
        val location = Location(
            tokenLocation.startLine,
            tokenLocation.startColumn + startIndex + end.length,
            tokenLocation.endLine,
            tokenLocation.endColumn - endDelta
        )
        return Pair(strValue, location)
    }

    private fun Token.getLocation(): Location {
        return Location(
            line,
            charPositionInLine,
            line,
            charPositionInLine + text.length - 1
        )
    }

    // endregion
}