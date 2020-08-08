package com.google.cloud.sqlecosystem.sqlextraction

import com.google.cloud.sqlecosystem.sqlextraction.antlr.Java9BaseVisitor
import com.google.cloud.sqlecosystem.sqlextraction.antlr.Java9Parser
import com.google.cloud.sqlecosystem.sqlextraction.output.Location
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.Token

/**
 * Analyzes a Java AST using the given data-flow engine
 *
 * @param dataFlowEngine engine running data-flow analysis
 */
class JavaAnalyzer(private val dataFlowEngine: DataFlowEngine) : Java9BaseVisitor<Unit>() {
    // region annotations

    override fun visitNormalAnnotation(ctx: Java9Parser.NormalAnnotationContext?) {
        ctx!!.typeName().accept(this)

        if (ctx.elementValuePairList() != null) {
            dataFlowEngine.visitAnnotation(
                Location.combine(
                    ctx.LPAREN().symbol.getLocation(),
                    ctx.RPAREN().symbol.getLocation()
                )
            ) { ctx.elementValuePairList().accept(this) }
        }
    }

    override fun visitSingleElementAnnotation(ctx: Java9Parser.SingleElementAnnotationContext?) {
        ctx!!.typeName().accept(this)

        dataFlowEngine.visitAnnotation(
            Location.combine(
                ctx.LPAREN().symbol.getLocation(),
                ctx.RPAREN().symbol.getLocation()
            )
        ) { ctx.elementValue().accept(this) }
    }

    // endregion

    // region methods

    override fun visitMethodDeclaration(ctx: Java9Parser.MethodDeclarationContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    override fun visitInstanceInitializer(ctx: Java9Parser.InstanceInitializerContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    override fun visitStaticInitializer(ctx: Java9Parser.StaticInitializerContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    override fun visitConstructorDeclaration(ctx: Java9Parser.ConstructorDeclarationContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    override fun visitLambdaExpression(ctx: Java9Parser.LambdaExpressionContext?) =
        dataFlowEngine.visitMethod { visitChildren(ctx) }

    // endregion

    // region method arguments

    override fun visitFormalParameter(ctx: Java9Parser.FormalParameterContext?) {
        dataFlowEngine.addMethodParameter(ctx!!.variableDeclaratorId().identifier().text)
        visitChildren(ctx)
    }

    override fun visitLastFormalParameter(ctx: Java9Parser.LastFormalParameterContext?) {
        if (ctx!!.variableDeclaratorId() != null) {
            dataFlowEngine.addMethodParameter(ctx.variableDeclaratorId().identifier().text)
        }
        visitChildren(ctx)
    }

    override fun visitLambdaParameters(ctx: Java9Parser.LambdaParametersContext?) {
        if (ctx!!.identifier() != null) {
            dataFlowEngine.addMethodParameter(ctx.identifier().text)
        }
        visitChildren(ctx)
    }

    override fun visitInferredFormalParameterList(
        ctx: Java9Parser.InferredFormalParameterListContext?
    ) {
        for (identifier in ctx!!.identifier()) {
            dataFlowEngine.addMethodParameter(identifier.text)
        }
    }

    // endregion

    // region statements

    override fun visitBlock(ctx: Java9Parser.BlockContext?) =
        dataFlowEngine.visitScope { visitChildren(ctx) }

    override fun visitFieldDeclaration(ctx: Java9Parser.FieldDeclarationContext?) =
        dataFlowEngine.visitStatement { visitChildren(ctx) }

    override fun visitLocalVariableDeclarationStatement(
        ctx: Java9Parser.LocalVariableDeclarationStatementContext?
    ) = dataFlowEngine.visitStatement { visitChildren(ctx) }

    override fun visitStatement(ctx: Java9Parser.StatementContext?) =
        dataFlowEngine.visitStatement { visitChildren(ctx) }

    // endregion

    override fun visitLiteral(ctx: Java9Parser.LiteralContext?) {
        val stringLiteral = ctx!!.StringLiteral()
        if (stringLiteral != null) {
            val literal = stringLiteral.text
            dataFlowEngine.visitStringLiteral(
                literal.substring(1, literal.length - 1),
                Location(
                    stringLiteral.symbol.line,
                    stringLiteral.symbol.charPositionInLine + 1,
                    stringLiteral.symbol.line,
                    stringLiteral.symbol.charPositionInLine + literal.length - 2
                )
            )
        }
    }

    override fun visitAdditiveExpression(ctx: Java9Parser.AdditiveExpressionContext?) {
        if (ctx!!.ADD() != null) {
            dataFlowEngine.visitConcatenation(
                { ctx.additiveExpression().accept(this) },
                { ctx.multiplicativeExpression().accept(this) })
        } else {
            visitChildren(ctx)
        }
    }

    // region variable declaration, assignment, and usage

    override fun visitVariableDeclaratorId(ctx: Java9Parser.VariableDeclaratorIdContext?) {
        dataFlowEngine.declareVariable(ctx!!.identifier().text)
    }

    override fun visitVariableDeclarator(ctx: Java9Parser.VariableDeclaratorContext?) {
        ctx!!.variableDeclaratorId().accept(this)
        if (ctx.ASSIGN() != null) {
            dataFlowEngine.visitAssignment(
                ctx.variableDeclaratorId().text,
                { ctx.variableInitializer().accept(this) })
        }
    }

    override fun visitAssignment(ctx: Java9Parser.AssignmentContext?) {
        dataFlowEngine.visitAssignment(
            ctx!!.leftHandSide().text,
            { ctx.expression().accept(this) },
            ctx.assignmentOperator().ADD_ASSIGN() != null
        )
    }

    override fun visitExpressionName(ctx: Java9Parser.ExpressionNameContext?) {
        dataFlowEngine.visitVariable(ctx!!.text)
    }

    // endregion

    override fun visitReturnStatement(ctx: Java9Parser.ReturnStatementContext?) {
        val retSymbol = ctx!!.RETURN().symbol
        dataFlowEngine.visitReturn(
            Location(
                retSymbol.line,
                retSymbol.charPositionInLine,
                retSymbol.line,
                retSymbol.charPositionInLine + retSymbol.text.length - 1
            )
        ) { visitChildren(ctx) }
    }

    // region method call

    override fun visitArgumentList(ctx: Java9Parser.ArgumentListContext?) {
        dataFlowEngine.visitMethodArguments(
            Location.combine(
                (ctx!!.parent as ParserRuleContext).getToken(Java9Parser.LPAREN, 0)
                    .symbol.getLocation(),
                (ctx.parent as ParserRuleContext).getToken(Java9Parser.RPAREN, 0)
                    .symbol.getLocation()
            ), ctx.expression().asSequence().map { arg -> { arg.accept(this) } })

    }

    // endregion

    // region private utils

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