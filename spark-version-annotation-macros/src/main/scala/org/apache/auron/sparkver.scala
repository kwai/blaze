/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron

import scala.annotation.StaticAnnotation
import scala.annotation.compileTimeOnly
import scala.language.experimental._
import scala.reflect.macros.whitebox

object sparkver {
  def matchVersion(vers: String): Boolean = {
    // please ensure the common module is built
    val configuredVer = org.apache.auron.common.ProjectConstants.SHIM_NAME
    for (ver <- vers.split("/")) {
      val verStripped = ver.trim
      if (s"spark-$verStripped" == configuredVer) {
        return true
      }
    }
    false
  }

  object Macros {
    def impl(c: whitebox.Context)(annottees: c.Expr[Any]*)(
        disabled: => c.Expr[Any]): c.Expr[Any] = {
      import c.universe._

      val versions = c.macroApplication match {
        case Apply(Select(Apply(_, List(vs)), _), _) => c.eval(c.Expr[String](q"$vs"))
      }

      if (matchVersion(versions)) {
        return c.Expr[Any](q"..$annottees")
      }
      disabled
    }

    def verEnable(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
      import c.universe._

      impl(c)(annottees: _*) {
        c.Expr(EmptyTree)
      }
    }

    def verEnableMembers(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
      import c.universe._

      impl(c)(annottees: _*) {
        val head = annottees.head.tree match {
          case ClassDef(mods, name, tparams, Template(parents, self, _body)) =>
            ClassDef(mods, name, tparams, Template(parents, self, List(EmptyTree)))
          case ModuleDef(mods, name, Template(parents, self, _body)) =>
            ModuleDef(mods, name, Template(parents, self, List(EmptyTree)))
        }
        c.Expr(q"$head; ..${annottees.tail}")
      }
    }

    def verEnableOverride(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
      import c.universe._
      import scala.reflect.internal.Flags
      impl(c)(annottees: _*) {
        val head = annottees.head.tree match {
          case DefDef(mods, name, tparams, vparams, tpt, rhs) =>
            val newMods = Modifiers(
              (mods.flags.asInstanceOf[Long] & ~Flags.OVERRIDE).asInstanceOf[FlagSet],
              mods.privateWithin,
              mods.annotations)
            DefDef(newMods, name, tparams, vparams, tpt, rhs)

          case ValDef(mods, name, tpt, rhs) =>
            val newMods = Modifiers(
              (mods.flags.asInstanceOf[Long] & ~Flags.OVERRIDE).asInstanceOf[FlagSet],
              mods.privateWithin,
              mods.annotations)
            ValDef(newMods, name, tpt, rhs)
        }
        c.Expr(q"$head; ..${annottees.tail}")
      }
    }
  }
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
final class sparkver(vers: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro sparkver.Macros.verEnable
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
final class sparkverEnableMembers(vers: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro sparkver.Macros.verEnableMembers
}

@compileTimeOnly("enable macro paradise to expand macro annotations")
final class sparkverEnableOverride(vers: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro sparkver.Macros.verEnableOverride
}
