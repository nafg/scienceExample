package com.howtographql.scala.sangria

import DBSchema._
import akka.http.scaladsl.model.DateTime
import com.howtographql.scala.sangria.models.{AuthProviderSignupData, Link, User, Vote}
import sangria.execution.deferred.{RelationIds, SimpleRelation}
import slick.jdbc.H2Profile.api._

import java.sql.Timestamp
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}
import com.howtographql.scala.sangria.DBSchema.dateTimeColumnType

import ExecutionContext.Implicits.global

class DAO(db: Database) {
  def allLinks = db.run(Links.result)

  def getLinks(ids: Seq[Int]): Future[Seq[Link]] = db.run(
    Links.filter(_.id inSet ids).result
  )

  def getLinksByUserIds(ids: Seq[Int]): Future[Seq[Link]] = {
    db.run {
      Links.filter(_.postedBy inSet ids).result
    }
  }

  def getUsers(ids: Seq[Int]): Future[Seq[User]] = db.run(
    Users.filter(_.id inSet ids).result
  )

  def getVotes(ids: Seq[Int]): Future[Seq[Vote]] = {
    db.run(
      Votes.filter(_.id inSet ids).result
    )
  }

  def getVotesByRelationIds(rel: RelationIds[Vote]): Future[Seq[Vote]] =
    db.run(
      Votes.filter { vote =>
        rel.rawIds.collect({
          case (SimpleRelation("byUser"), ids: Seq[Int]) => vote.userId inSet ids
          case (SimpleRelation("byLink"), ids: Seq[Int]) => vote.linkId inSet ids
        }).foldLeft(true: Rep[Boolean])(_ || _)

      } result
    )

  def createUser(name: String, authProvider: AuthProviderSignupData): Future[User] = {
    val newUser = User(0, name, authProvider.email.email, authProvider.email.password)

    val insertAndReturnUserQuery = (Users returning Users.map(_.id)) into {
      (user, id) => user.copy(id = id)
    }

    db.run {
      insertAndReturnUserQuery += newUser
    }
  }

  def createLink(url: String, description: String, postedBy: Int): Future[Link] = {

    val insertAndReturnLinkQuery = (Links returning Links.map(_.id)) into {
      (link, id) => link.copy(id = id)
    }
    db.run {
      insertAndReturnLinkQuery += Link(0, url, description, postedBy)
    }
  }

  def createVote(linkId: Int, userId: Int): Future[Vote] = {
    val insertAndReturnVoteQuery = (Votes returning Votes.map(_.id)) into {
      (vote, id) => vote.copy(id = id)
    }
    db.run {
      insertAndReturnVoteQuery += Vote(0, userId, linkId)
    }
  }

  def authenticate(email: String, password: String): Future[Option[User]] = db.run {
    Users.filter(u => u.email === email && u.password === password).result.headOption
  }


  def collectTodayPost(ids: Seq[Int]) = { // TODO add fetcher
    for {
      isFresh <- isFreshPost(ids)
      todayPosts <- getTodayPosts(isFresh)
      _ <- writeTodayPosts(todayPosts) // TODO IMPLEMENT
    } yield true
  }

  def isFreshPost(ids: Seq[Int]): Future[Seq[(Int, Boolean)]] = {// todayLinksFetcher at src/main/scala/com/howtographql/scala/sangria/GraphQLSchema.scala
    db.run {
      val now = DateTime.MinValue
      val startOfTheDay = DateTime(now.year, now.month, now.day)
      (Links.filter(_.postedBy inSet ids).filter(_.createdAt >= startOfTheDay).map(l => (l.id, true)) ++
        Links.filter(_.postedBy inSet ids).filter(_.createdAt < startOfTheDay).map(l => (l.id, false))).result
    }
  }

  def getTodayPosts(isFresh: Seq[(Int, Boolean)]) = {
    val query = isFresh.map {
      labelId =>
        Links.filter(_.id === labelId._1)
          .join(Users).on(_.postedBy == _.id).
          map(l => (l._1.id, l._1.url, labelId._2 match {
            case true => l._1.description
            case false => "" // empty description if not fresh, TODO FIX
          }, l._1.createdAt, l._2.name))
    }
    db run query.result // unable to run Seq[Query[Nothing, Nothing, Seq]], TODO FIX
  }

}
