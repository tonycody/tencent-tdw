/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;

public class RollupAndCubeTranslator {
  
  private boolean cubeNullFilterEnable = false;

  public void changeASTTreesForRC(ASTNode ast) throws SemanticException {

    HiveConf conf = SessionState.get().getConf();
    cubeNullFilterEnable = conf.getBoolean("hive.cube.null.filter.enable", false);
    
    if (ast.getToken().getType() == HiveParser.TOK_QUERY) {
      ASTNode from = (ASTNode) ast.getChild(0);
      ASTNode fromChild = (ASTNode) from.getChild(0);
      ASTNode fromGrandSon = (ASTNode) fromChild.getChild(0);

      if (fromGrandSon.getToken().getType() == HiveParser.TOK_UNION
          || fromGrandSon.getToken().getType() == HiveParser.TOK_UNIQUE_UNION) {

        detectAndTranslateInUnion(fromGrandSon);
      }

      if (ast.getChildCount() == 2) {
        if (detectRollupAndCube(ast)) {
          translateRollupAndCube(ast);
        }

      } else {
        if (detectRollupAndCube(ast))
          throw new SemanticException("Not support multi-insert clauses!");
      }

    }

    int child_count = ast.getChildCount();
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {

      changeASTTreesForRC((ASTNode) ast.getChild(child_pos));
    }
  }

  private void detectAndTranslateInUnion(ASTNode uni) throws SemanticException {
    ASTNode child0 = (ASTNode) uni.getChild(0);
    ASTNode child1 = (ASTNode) uni.getChild(1);

    if (child0.getToken().getType() == HiveParser.TOK_UNION
        | child0.getToken().getType() == HiveParser.TOK_UNIQUE_UNION) {
      detectAndTranslateInUnion(child0);
    } else if (detectRollupAndCube(child0)) {
      translateRollupAndCubeInUnion(uni, 0);
    }

    if (child1.getToken().getType() == HiveParser.TOK_UNION
        | child1.getToken().getType() == HiveParser.TOK_UNIQUE_UNION) {
      detectAndTranslateInUnion(child1);
    } else if (detectRollupAndCube(child1)) {
      translateRollupAndCubeInUnion(uni, 1);
    }
  }

  private boolean detectRollupAndCube(ASTNode ast) {

    for (int i = 0; i < ast.getChildCount(); i++) {
      ASTNode insert = (ASTNode) ast.getChild(i);

      if (insert.getToken().getType() == HiveParser.TOK_INSERT) {
        for (int j = 0; j < insert.getChildCount(); j++) {
          ASTNode groupBy = (ASTNode) insert.getChild(j);

          if (groupBy.getToken().getType() == HiveParser.TOK_GROUPBY) {
            for (int k = 0; k < groupBy.getChildCount(); k++) {
              ASTNode current = (ASTNode) groupBy.getChild(k);

              if (current.getToken().getType() == HiveParser.TOK_ROLLUP
                  || current.getToken().getType() == HiveParser.TOK_CUBE) {
                return true;
              }
            }
          }
        }
      }
    }

    return false;
  }

  private void translateRollupAndCube(ASTNode ast) throws SemanticException {
    GroupByInfo info = collectGroupByInfo(ast);

    ArrayList<ASTNode> newTrees = new ArrayList<ASTNode>();
    ArrayList<ASTNode> groupBys = info.getGroupBy();

    ArrayList<ArrayList<ArrayList<ASTNode>>> newGroupbys = new ArrayList<ArrayList<ArrayList<ASTNode>>>(
        groupBys.size());

    for (int i = 0; i < groupBys.size(); i++) {
      newGroupbys.add(new ArrayList<ArrayList<ASTNode>>());
      ArrayList<ArrayList<ASTNode>> layer1 = newGroupbys.get(i);

      ASTNode groupBy = groupBys.get(i);

      if (groupBy.getToken().getType() == HiveParser.TOK_ROLLUP) {
        int size = groupBy.getChildCount();

        for (int j = 0; j < size; j++) {
          layer1.add(new ArrayList<ASTNode>());
        }
        layer1.add(null);

        for (int j = 0; j < size; j++) {
          ASTNode jgroup = (ASTNode) groupBy.getChild(j);

          ArrayList<ASTNode> jnodes = new ArrayList<ASTNode>();

          if (jgroup.getToken().getType() == HiveParser.TOK_GROUP) {
            jnodes.clear();
            for (int nz = 0; nz < jgroup.getChildCount(); nz++) {
              ASTNode theNode = (ASTNode) jgroup.getChild(nz);
              jnodes.add(theNode);
              info.setInRollupAndCube(theNode.toStringTree().toLowerCase());
            }

            for (int jj = 0; jj < size - j; jj++) {
              ArrayList<ASTNode> layer2 = layer1.get(jj);
              for (int nz = 0; nz < jnodes.size(); nz++) {
                layer2.add(jnodes.get(nz));
              }
            }

          } else {
            info.setInRollupAndCube(jgroup.toStringTree().toLowerCase());

            for (int jj = 0; jj < size - j; jj++) {
              ArrayList<ASTNode> layer2 = layer1.get(jj);
              layer2.add(jgroup);
            }
          }

        }

      } else if (groupBy.getToken().getType() == HiveParser.TOK_CUBE) {
        int size = groupBy.getChildCount();

        for (int j = 0; j < Math.pow(2, size) - 1; j++) {
          layer1.add(new ArrayList<ASTNode>());
        }
        layer1.add(null);

        getComposition(groupBy, layer1);

        for (int cc = 0; cc < size; cc++) {
          ASTNode cCode = (ASTNode) groupBy.getChild(cc);

          if (cCode.getToken().getType() == HiveParser.TOK_GROUP) {

            for (int nz = 0; nz < cCode.getChildCount(); nz++) {
              ASTNode theNode = (ASTNode) cCode.getChild(nz);
              info.setInRollupAndCube(theNode.toStringTree().toLowerCase());
            }

          } else {
            info.setInRollupAndCube(cCode.toStringTree().toLowerCase());
          }
        }

      } else {
        layer1.add(new ArrayList<ASTNode>());
        layer1.get(0).add(groupBy);
      }
    }

    int count = 1;
    for (int i = 0; i < newGroupbys.size(); i++)
      count *= newGroupbys.get(i).size();

    for (int i = 0; i < newGroupbys.size(); i++) {
      for (int j = 0; j < newGroupbys.get(i).size(); j++) {
        String out = "The " + j + "th Composition: ";
        if (newGroupbys.get(i).get(j) != null) {
          for (int k = 0; k < newGroupbys.get(i).get(j).size(); k++) {
            out += newGroupbys.get(i).get(j).get(k).toStringTree() + " + ";
          }
        }

      }
    }

    HashSet<String> hashset = new HashSet<String>();
    ArrayList<ArrayList<ASTNode>> groupbys = getColumnComposition(newGroupbys,
        0, hashset);

    ArrayList<ASTNode> newGroupBys = new ArrayList<ASTNode>();
    for (int i = 0; i < count; i++) {
      newGroupBys.add(new ASTNode(new CommonToken(HiveParser.TOK_GROUPBY,
          "TOK_GROUPBY")));
      ASTNode gb = newGroupBys.get(i);

      ArrayList<ASTNode> groupby = groupbys.get(i);

      if (groupby != null) {
        for (int j = 0; j < groupby.size(); j++) {
          gb.addChild(groupby.get(j));
        }
      }

    }

    translateRollupAndCubeInQuery(ast, info, newGroupBys);
  }

  private void translateRollupAndCubeInQuery(ASTNode ast, GroupByInfo info,
      ArrayList<ASTNode> newGroupBys) throws SemanticException {
    int size = newGroupBys.size();
    ArrayList<ASTNode> trees = new ArrayList<ASTNode>();

    ArrayList<String> allPossibleGBs = new ArrayList<String>();
    ASTNode theFirst = newGroupBys.get(0);
    for (int i = 0; i < theFirst.getChildCount(); i++) {
      allPossibleGBs.add(((ASTNode) theFirst.getChild(i)).toStringTree()
          .toLowerCase());
    }
    ArrayList<String> inRollupAndCube = info.getInRollupAndCube();

    for (int i = 0; i < size; i++) {
      ASTNode newTree = new ASTNode(new CommonToken(HiveParser.TOK_QUERY,
          "TOK_QUERY"));
      trees.add(newTree);

      newTree.addChild(info.getFrom());

      newTree.addChild(new ASTNode(new CommonToken(HiveParser.TOK_INSERT,
          "TOK_INSERT")));

      ASTNode insert = (ASTNode) newTree.getChild(1);
      int s = 0;

      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_DESTINATION,
          "TOK_DESTINATION")));
      ASTNode subDest = (ASTNode) insert.getChild(0);
      subDest.addChild(new ASTNode(new CommonToken(HiveParser.TOK_DIR,
          "TOK_DIR")));
      ASTNode subDir = (ASTNode) subDest.getChild(0);
      subDir.addChild(new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE,
          "TOK_TMP_FILE")));

      s++;

      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
          "TOK_SELECT")));
      s++;

      ASTNode select = (ASTNode) insert.getChild(s - 1);
      ArrayList<GroupingPosition> gps = new ArrayList<GroupingPosition>();
      ArrayList<ASTNode> sels = info.getSelect();
      int j = 0;

      for (ASTNode sel : sels) {
        ASTNode newSel = dupASTNode(sel);

        select.addChild(newSel);
        j++;
      }

      if (info.getWhere() != null) {
        insert.addChild(info.getWhere());
        s++;
      }

      findAllGrouping(select, gps);

      ASTNode gb = newGroupBys.get(i);
      if (gb.getChildCount() != 0) {
        insert.addChild(gb);
        s++;
      } else
        gb = null;

      ArrayList<String> allCurrentGBs = new ArrayList<String>();
      if (gb != null) {
        for (int k = 0; k < gb.getChildCount(); k++) {
          allCurrentGBs.add(((ASTNode) gb.getChild(k)).toStringTree()
              .toLowerCase());
        }
      }

      HashSet<String> toConstitutes = new HashSet<String>();
      for (int k = 0; k < select.getChildCount(); k++) {
        String gbstring = ((ASTNode) select.getChild(k)).toStringTree()
            .toLowerCase();
        toConstitutes.clear();
        boolean containsGrouping = gbstring.contains("(tok_function grouping");

        for (String test : allPossibleGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.add(test);

          }
        }

        for (String test : allCurrentGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.remove(test);

          }
        }

        if (!toConstitutes.isEmpty() && !containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);
          if(cubeNullFilterEnable){
            translateColToNull(toChange);
          }
          else{
            toChange.setChild(0, new ASTNode(new CommonToken(HiveParser.TOK_NULL,
                "TOK_NULL")));
          }
        } else if (!toConstitutes.isEmpty() && containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);

          for (String toConstitute : toConstitutes) {
            constituteDescendant(toChange, toConstitute);
          }
        }
      }

      if (gb != null) {

        for (GroupingPosition gp : gps) {
          ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
          assert grouping.getChildCount() == 2;
          ASTNode groupingPara = (ASTNode) grouping.getChild(1);
          boolean changed = false;
          boolean inAll = false;
          String groupingParaStr = groupingPara.toStringTree().toLowerCase();

          for (String test : inRollupAndCube) {
            if (test.equals(groupingParaStr)) {
              inAll = true;
              break;
            }
          }
          if (!inAll) {
            throw new SemanticException("The grouping parameter: "
                + ((ASTNode) groupingPara.getChild(0)).toStringTree()
                + " is not a parameter of rollup or cube!");
          }

          int gpsize = gb.getChildCount();
          for (int gbi = 0; gbi < gpsize; gbi++) {
            ASTNode gbinode = (ASTNode) gb.getChild(gbi);

            if (gbinode.toStringTree().equalsIgnoreCase(
                groupingPara.toStringTree())) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "0")));
              changed = true;
              break;
            }

          }

          if (!changed) {
            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }

      } else {
        for (GroupingPosition gp : gps) {

          gp.father.setChild(gp.position, new ASTNode(new CommonToken(
              HiveParser.Number, "1")));
        }

        insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_GROUPBY,
            "TOK_GROUPBY")));
        s++;
        ASTNode pseudoGroupBy = (ASTNode) insert.getChild(s - 1);
        pseudoGroupBy.addChild(new ASTNode(new CommonToken(HiveParser.Number,
            "0")));
      }

      if (info.getHaving() != null) {
        ASTNode newHaving = dupASTNode(info.getHaving());
        insert.addChild(newHaving);
        s++;

        gps.clear();
        findAllGrouping(newHaving, gps);

        if (gb != null) {
          for (GroupingPosition gp : gps) {
            ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
            assert grouping.getChildCount() == 2;
            ASTNode groupingPara = (ASTNode) grouping.getChild(1);
            boolean changed = false;

            int gpsize = gb.getChildCount();
            for (int gbi = 0; gbi < gpsize; gbi++) {
              ASTNode gbinode = (ASTNode) gb.getChild(gbi);

              if (gbinode.toStringTree().equalsIgnoreCase(
                  groupingPara.toStringTree())) {
                gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                    HiveParser.Number, "0")));
                changed = true;
                break;
              }

            }

            if (!changed) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "1")));
            }
          }

        } else {
          for (GroupingPosition gp : gps) {

            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }
      }
    }

    ASTNode sonUnion = trees.get(0);

    for (int i = 1; i < size; i++) {
      ASTNode fatherUnion = new ASTNode(new CommonToken(HiveParser.TOK_UNION,
          "TOK_UNION"));
      fatherUnion.addChild(sonUnion);
      fatherUnion.addChild(trees.get(i));
      sonUnion = fatherUnion;
    }

    int astChildCount = ast.getChildCount();
    for (int i = astChildCount - 1; i >= 0; i--)
      ast.deleteChild(i);
    ast.addChild(new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM")));
    ASTNode subFrom = (ASTNode) ast.getChild(0);
    subFrom.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY,
        "TOK_SUBQUERY")));
    ASTNode subQuery = (ASTNode) subFrom.getChild(0);

    subQuery.addChild(sonUnion);

    Random rand = new Random();
    String queryName = "union_result_" + rand.nextInt(10) + rand.nextInt(10)
        + rand.nextInt(10) + rand.nextInt(10);
    subQuery.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
        queryName)));

    ast.addChild(new ASTNode(new CommonToken(HiveParser.TOK_INSERT,
        "TOK_INSERT")));
    ASTNode subInsert = (ASTNode) ast.getChild(1);

    subInsert.addChild(info.getDestination());

    subInsert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
        "TOK_SELECT")));
    ASTNode subSelect = (ASTNode) subInsert.getChild(1);
    subSelect.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR,
        "TOK_SELEXPR")));
    ASTNode subSelexpr = (ASTNode) subSelect.getChild(0);
    subSelexpr.addChild(new ASTNode(new CommonToken(HiveParser.TOK_ALLCOLREF,
        "TOK_ALLCOLREF")));

    int subi = 2;
    for (ASTNode node : info.getOtherInsertNodes()) {
      subInsert.addChild(node);
      subi++;
    }

  }
  
  private void translateColToNull(ASTNode toChange){
    int childSize = toChange.getChildCount();
    for(int i = 0; i < childSize; i++){
      ASTNode n = (ASTNode)toChange.getChild(i);
      if(n.getToken().getType() == HiveParser.TOK_TABLE_OR_COL){
        toChange.setChild(i, new ASTNode(new CommonToken(HiveParser.TOK_NULL, "TOK_NULL")));
        continue;
      }
      else{
        translateColToNull(n);
      }
    }
  }

  private void translateRollupAndCubeInUnion(ASTNode union, int idx)
      throws SemanticException {
    ASTNode query = (ASTNode) union.getChild(idx);

    GroupByInfo info = collectGroupByInfo(query);

    ArrayList<ASTNode> newTrees = new ArrayList<ASTNode>();
    ArrayList<ASTNode> groupBys = info.getGroupBy();

    ArrayList<ArrayList<ArrayList<ASTNode>>> newGroupbys = new ArrayList<ArrayList<ArrayList<ASTNode>>>(
        groupBys.size());

    for (int i = 0; i < groupBys.size(); i++) {
      newGroupbys.add(new ArrayList<ArrayList<ASTNode>>());
      ArrayList<ArrayList<ASTNode>> layer1 = newGroupbys.get(i);

      ASTNode groupBy = groupBys.get(i);

      if (groupBy.getToken().getType() == HiveParser.TOK_ROLLUP) {
        int size = groupBy.getChildCount();

        for (int j = 0; j < size; j++) {
          layer1.add(new ArrayList<ASTNode>());
        }
        layer1.add(null);

        for (int j = 0; j < size; j++) {
          ASTNode jgroup = (ASTNode) groupBy.getChild(j);
          ArrayList<ASTNode> jnodes = new ArrayList<ASTNode>();

          if (jgroup.getToken().getType() == HiveParser.TOK_GROUP) {
            jnodes.clear();
            for (int nz = 0; nz < jgroup.getChildCount(); nz++) {
              ASTNode theNode = (ASTNode) jgroup.getChild(nz);
              jnodes.add(theNode);
              info.setInRollupAndCube(theNode.toStringTree().toLowerCase());
            }

            for (int jj = 0; jj < size - j; jj++) {
              ArrayList<ASTNode> layer2 = layer1.get(jj);
              for (int nz = 0; nz < jnodes.size(); nz++) {
                layer2.add(jnodes.get(nz));
              }
            }

          } else {
            info.setInRollupAndCube(jgroup.toStringTree().toLowerCase());

            for (int jj = 0; jj < size - j; jj++) {
              ArrayList<ASTNode> layer2 = layer1.get(jj);
              layer2.add(jgroup);
            }
          }

        }

      } else if (groupBy.getToken().getType() == HiveParser.TOK_CUBE) {
        int size = groupBy.getChildCount();

        for (int j = 1; j < Math.pow(2, size); j++) {
          layer1.add(new ArrayList<ASTNode>());
        }
        layer1.add(null);

        getComposition(groupBy, layer1);

        for (int cc = 0; cc < size; cc++) {
          ASTNode cCode = (ASTNode) groupBy.getChild(cc);

          if (cCode.getToken().getType() == HiveParser.TOK_GROUP) {

            for (int nz = 0; nz < cCode.getChildCount(); nz++) {
              ASTNode theNode = (ASTNode) cCode.getChild(nz);
              info.setInRollupAndCube(theNode.toStringTree().toLowerCase());
            }

          } else {
            info.setInRollupAndCube(cCode.toStringTree().toLowerCase());
          }
        }

      } else {
        layer1.add(new ArrayList<ASTNode>());
        layer1.get(0).add(groupBy);
      }
    }

    int count = 1;
    for (int i = 0; i < newGroupbys.size(); i++)
      count *= newGroupbys.get(i).size();

    HashSet<String> hashset = new HashSet<String>();
    ArrayList<ArrayList<ASTNode>> groupbys = getColumnComposition(newGroupbys,
        0, hashset);

    ArrayList<ASTNode> newGroupBys = new ArrayList<ASTNode>();
    for (int i = 0; i < count; i++) {
      newGroupBys.add(new ASTNode(new CommonToken(HiveParser.TOK_GROUPBY,
          "TOK_GROUPBY")));
      ASTNode gb = newGroupBys.get(i);

      ArrayList<ASTNode> groupby = groupbys.get(i);

      if (groupby != null) {
        for (int j = 0; j < groupby.size(); j++) {
          gb.addChild(groupby.get(j));
        }
      }

    }

    boolean toTmpFile = false;
    if (info.getDestination().toStringTree()
        .equalsIgnoreCase("(TOK_DESTINATION (TOK_DIR TOK_TMP_FILE))"))
      toTmpFile = true;

    if (info.getOtherInsertNodes().size() == 0 && toTmpFile) {
      translateRollupAndCubeInUnionWithoutSelect(union, idx, info, newGroupBys);
    } else
      translateRollupAndCubeInUnionWithSelect(union, idx, info, newGroupBys);
  }

  static GroupByInfo collectGroupByInfo(ASTNode query) throws SemanticException {
    GroupByInfo info = new GroupByInfo();
    int idx = 0;

    ASTNode from = (ASTNode) query.getChild(0);
    if (from.getToken().getType() == HiveParser.TOK_FROM) {
      if (from.getChildCount() != 1)
        throw new SemanticException("Wrong Multidimensions Analysis SQL: "
            + query.toStringTree());
      info.setFrom(from);

    } else
      throw new SemanticException("Wrong Multidimensions Analysis SQL: "
          + query.toStringTree());

    ASTNode insert = (ASTNode) query.getChild(1);

    ASTNode destination = (ASTNode) insert.getChild(idx++);
    if ((destination.getToken().getType() != HiveParser.TOK_DESTINATION)
        && (destination.getToken().getType() != HiveParser.TOK_APPENDDESTINATION))
      throw new SemanticException("Wrong Multidimensions Analysis SQL: "
          + query.toStringTree());
    info.setDestination(destination);

    ASTNode selects = (ASTNode) insert.getChild(idx++);
    if ((selects.getToken().getType() != HiveParser.TOK_SELECT)
        && (selects.getToken().getType() != HiveParser.TOK_SELECTDI))
      throw new SemanticException("Wrong Multidimensions Analysis SQL: "
          + query.toStringTree());

    if (selects.getToken().getType() == HiveParser.TOK_SELECTDI)
      throw new SemanticException(
          "Not support select distinct in Multidimensions Analysis!");

    for (int i = 0; i < selects.getChildCount(); i++) {
      ASTNode select = (ASTNode) selects.getChild(i);
      if (select.toStringTree().equalsIgnoreCase("(TOK_SELEXPR TOK_ALLCOLREF)"))
        throw new SemanticException(
            "Not support select * in Multidimensions Analysis!");

      info.setSelect(select);
    }

    if (((ASTNode) insert.getChild(idx)).getToken().getType() == HiveParser.TOK_WHERE) {
      ASTNode where = (ASTNode) insert.getChild(idx++);
      info.setWhere(where);

    }

    if (((ASTNode) insert.getChild(idx)).getToken().getType() == HiveParser.TOK_GROUPBY) {
      ASTNode groupBy = (ASTNode) insert.getChild(idx++);

      for (int i = 0; i < groupBy.getChildCount(); i++) {
        info.setGroupBy((ASTNode) groupBy.getChild(i));
      }
    }

    if (idx < insert.getChildCount()) {
      if (((ASTNode) insert.getChild(idx)).getToken().getType() == HiveParser.TOK_HAVING) {
        ASTNode having = (ASTNode) insert.getChild(idx++);
        info.setHaving(having);

      }
    }

    for (; idx < insert.getChildCount(); idx++) {
      info.setOtherInsertNodes((ASTNode) insert.getChild(idx));
    }

    return info;
  }

  public void getComposition(ASTNode cube, ArrayList<ArrayList<ASTNode>> layer1) {
    int idx = 0;
    ArrayList<ASTNode> allNodes = new ArrayList<ASTNode>();

    for (int i = 0; i < cube.getChildCount(); i++)
      allNodes.add((ASTNode) cube.getChild(i));

    int n = allNodes.size();
    for (int m = n; m > 0; m--) {
      idx = choose(allNodes, layer1, idx, n, m);
    }

    for (int i = 0; i < layer1.size(); i++) {
      ArrayList<ASTNode> layer2 = layer1.get(i);

      String re = " ";
      if (layer2 != null) {
        for (int j = 0; j < layer2.size(); j++) {
          re += "    +    " + layer2.get(j).toStringTree();
        }
      }

    }
  }

  public int choose(ArrayList<ASTNode> allNodes,
      ArrayList<ArrayList<ASTNode>> layer1, int idx, int n, int m) {
    int[] init = new int[n];
    for (int i = 1; i <= n; i++)
      init[i - 1] = i;
    int[] end = new int[m];
    int trace = 0;

    for (int i = 0; i < n;) {
      if (n - i == m - trace) {
        for (int j = 0; j < m - trace; j++) {
          end[trace + j] = init[i + j];
        }

        ArrayList<ASTNode> toAdd = layer1.get(idx++);
        for (int j = 0; j < m; j++) {
          ASTNode now = allNodes.get(end[j] - 1);
          if (now.getToken().getType() == HiveParser.TOK_GROUP) {
            for (int k = 0; k < now.getChildCount(); k++) {
              toAdd.add((ASTNode) now.getChild(k));
            }
          } else
            toAdd.add(now);
        }

        if (trace == 0)
          break;
        i = end[--trace];
      } else if (trace == m) {
        ArrayList<ASTNode> toAdd = layer1.get(idx++);
        for (int j = 0; j < m; j++) {
          ASTNode now = allNodes.get(end[j] - 1);
          if (now.getToken().getType() == HiveParser.TOK_GROUP) {
            for (int k = 0; k < now.getChildCount(); k++) {
              toAdd.add((ASTNode) now.getChild(k));
            }
          } else
            toAdd.add(now);

        }

        i = end[--trace];
      } else
        end[trace++] = init[i++];
    }

    return idx;
  }

  ArrayList<ArrayList<ASTNode>> getColumnComposition(
      ArrayList<ArrayList<ArrayList<ASTNode>>> newGroupbys, int i,
      HashSet<String> hashset) {
    hashset.clear();

    if (i == newGroupbys.size() - 1) {
      ArrayList<ArrayList<ASTNode>> old = new ArrayList<ArrayList<ASTNode>>();

      for (int j = 0; j < newGroupbys.get(i).size(); j++) {
        hashset.clear();

        old.add(new ArrayList<ASTNode>());
        ArrayList<ASTNode> groupj = old.get(j);

        if (newGroupbys.get(i).get(j) != null) {
          for (int k = 0; k < newGroupbys.get(i).get(j).size(); k++) {
            if (!hashset.contains(newGroupbys.get(i).get(j).get(k)
                .toStringTree().toLowerCase())) {
              groupj.add(newGroupbys.get(i).get(j).get(k));
              hashset.add(newGroupbys.get(i).get(j).get(k).toStringTree()
                  .toLowerCase());
            }
          }
        }

      }

      return old;

    } else {
      ArrayList<ArrayList<ASTNode>> oldCompositions = getColumnComposition(
          newGroupbys, i + 1, hashset);
      int oldsize = oldCompositions.size();

      ArrayList<ArrayList<ASTNode>> columni = newGroupbys.get(i);
      int sizei = columni.size();

      ArrayList<ArrayList<ASTNode>> newCompositions = new ArrayList<ArrayList<ASTNode>>(
          sizei * oldsize);

      for (int j = 0; j < sizei; j++) {

        for (int k = 0; k < oldsize; k++) {
          hashset.clear();
          ArrayList<ASTNode> newlist = new ArrayList<ASTNode>();

          if (columni.get(j) != null) {
            for (int h = 0; h < columni.get(j).size(); h++) {

              if (columni.get(j).get(h) != null) {
                if (!hashset.contains(columni.get(j).get(h).toStringTree()
                    .toLowerCase())) {
                  newlist.add(columni.get(j).get(h));
                  hashset.add(columni.get(j).get(h).toStringTree()
                      .toLowerCase());
                }
              }

            }
          }

          if (oldCompositions.get(k) != null) {
            for (int h = 0; h < oldCompositions.get(k).size(); h++) {
              if (!hashset.contains(oldCompositions.get(k).get(h)
                  .toStringTree().toLowerCase())) {
                newlist.add(oldCompositions.get(k).get(h));
                hashset.add(oldCompositions.get(k).get(h).toStringTree()
                    .toLowerCase());
              }
            }
          }

          newCompositions.add(newlist);
        }
      }

      return newCompositions;
    }

  }

  private ASTNode dupASTNode(ASTNode original) {
    ASTNode newNode = new ASTNode(new CommonToken(original.getType(),
        original.getText()));
    int count = original.getChildCount();
    if (count != 0) {
      for (int i = 0; i < count; i++) {
        newNode.addChild(dupASTNode((ASTNode) original.getChild(i)));
      }
    }
    return newNode;
  }

  private void findAllGrouping(ASTNode selectNode,
      ArrayList<GroupingPosition> gps) {
    int count = selectNode.getChildCount();

    if (selectNode.getChildCount() > 0) {
      for (int i = 0; i < count; i++) {
        ASTNode node = (ASTNode) selectNode.getChild(i);

        if (node.toStringTree().toLowerCase()
            .startsWith("(tok_function grouping")) {
          GroupingPosition gp = new GroupingPosition();
          gp.father = selectNode;
          gp.position = i;

          gps.add(gp);

        } else {
          findAllGrouping(node, gps);
        }
      }
    }
  }

  class GroupingPosition {
    ASTNode father = null;
    int position = -1;
  }

  private void constituteDescendant(ASTNode toChange, String toConstitute) {

    if (toChange.getChildCount() != 0) {
      boolean isGrouping = toChange.toStringTree().toLowerCase()
          .startsWith("(tok_function grouping");

      if (!isGrouping) {

        for (int i = 0; i < toChange.getChildCount(); i++) {
          ASTNode des = (ASTNode) toChange.getChild(i);

          if (des.toStringTree().toLowerCase().startsWith(toConstitute)
              || des.toStringTree().toLowerCase().equals(toConstitute)) {
            toChange.setChild(i, new ASTNode(new CommonToken(
                HiveParser.TOK_NULL, "TOK_NULL")));
          } else
            constituteDescendant(des, toConstitute);

        }
      }

    }
  }

  private void translateRollupAndCubeInUnionWithoutSelect(ASTNode union,
      int idx, GroupByInfo info, ArrayList<ASTNode> newGroupBys)
      throws SemanticException {
    int size = newGroupBys.size();
    ArrayList<ASTNode> trees = new ArrayList<ASTNode>();

    ArrayList<String> allPossibleGBs = new ArrayList<String>();
    ASTNode theFirst = newGroupBys.get(0);
    for (int i = 0; i < theFirst.getChildCount(); i++) {
      allPossibleGBs.add(((ASTNode) theFirst.getChild(i)).toStringTree()
          .toLowerCase());
    }
    ArrayList<String> inRollupAndCube = info.getInRollupAndCube();

    for (int i = 0; i < size; i++) {
      ASTNode newTree = new ASTNode(new CommonToken(HiveParser.TOK_QUERY,
          "TOK_QUERY"));
      trees.add(newTree);

      newTree.addChild(info.getFrom());

      newTree.addChild(new ASTNode(new CommonToken(HiveParser.TOK_INSERT,
          "TOK_INSERT")));

      ASTNode insert = (ASTNode) newTree.getChild(1);
      int s = 0;

      insert.addChild(info.getDestination());
      s++;

      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
          "TOK_SELECT")));
      s++;

      ASTNode select = (ASTNode) insert.getChild(s - 1);
      ArrayList<GroupingPosition> gps = new ArrayList<GroupingPosition>();
      ArrayList<ASTNode> sels = info.getSelect();
      int j = 0;

      for (ASTNode sel : sels) {
        ASTNode newSel = dupASTNode(sel);

        select.addChild(newSel);
        j++;
      }

      if (info.getWhere() != null) {
        insert.addChild(info.getWhere());
        s++;
      }

      findAllGrouping(select, gps);

      ASTNode gb = newGroupBys.get(i);
      if (gb.getChildCount() != 0) {
        insert.addChild(gb);
        s++;
      } else
        gb = null;

      ArrayList<String> allCurrentGBs = new ArrayList<String>();
      if (gb != null) {
        for (int k = 0; k < gb.getChildCount(); k++) {
          allCurrentGBs.add(((ASTNode) gb.getChild(k)).toStringTree()
              .toLowerCase());
        }
      }

      HashSet<String> toConstitutes = new HashSet<String>();
      for (int k = 0; k < select.getChildCount(); k++) {
        String gbstring = ((ASTNode) select.getChild(k)).toStringTree()
            .toLowerCase();
        toConstitutes.clear();
        boolean containsGrouping = gbstring.contains("(tok_function grouping");

        for (String test : allPossibleGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.add(test);
          }
        }

        for (String test : allCurrentGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.remove(test);
          }
        }

        if (!toConstitutes.isEmpty() && !containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);

          if(cubeNullFilterEnable){
            translateColToNull(toChange);
          }
          else{
            toChange.setChild(0, new ASTNode(new CommonToken(HiveParser.TOK_NULL,
                "TOK_NULL")));
          }
          translateColToNull(toChange);

        } else if (!toConstitutes.isEmpty() && containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);

          for (String toConstitute : toConstitutes) {
            constituteDescendant(toChange, toConstitute);
          }
        }
      }

      if (gb != null) {
        for (GroupingPosition gp : gps) {
          ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
          assert grouping.getChildCount() == 2;
          ASTNode groupingPara = (ASTNode) grouping.getChild(1);
          boolean changed = false;
          boolean inAll = false;
          String groupingParaStr = groupingPara.toStringTree().toLowerCase();

          for (String test : inRollupAndCube) {
            if (test.equals(groupingParaStr)) {
              inAll = true;
              break;
            }
          }
          if (!inAll) {
            throw new SemanticException("The grouping parameter: "
                + ((ASTNode) groupingPara.getChild(0)).toStringTree()
                + " is not a parameter of rollup or cube!");
          }

          int gpsize = gb.getChildCount();
          for (int gbi = 0; gbi < gpsize; gbi++) {
            ASTNode gbinode = (ASTNode) gb.getChild(gbi);

            if (gbinode.toStringTree().equalsIgnoreCase(
                groupingPara.toStringTree())) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "0")));
              changed = true;
              break;
            }

          }

          if (!changed) {
            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }

      } else {
        for (GroupingPosition gp : gps) {

          gp.father.setChild(gp.position, new ASTNode(new CommonToken(
              HiveParser.Number, "1")));
        }

        insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_GROUPBY,
            "TOK_GROUPBY")));
        s++;
        ASTNode pseudoGroupBy = (ASTNode) insert.getChild(s - 1);
        pseudoGroupBy.addChild(new ASTNode(new CommonToken(HiveParser.Number,
            "0")));
      }

      if (info.getHaving() != null) {
        ASTNode newHaving = dupASTNode(info.getHaving());
        insert.addChild(newHaving);
        s++;

        gps.clear();
        findAllGrouping(newHaving, gps);

        if (gb != null) {
          for (GroupingPosition gp : gps) {
            ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
            assert grouping.getChildCount() == 2;
            ASTNode groupingPara = (ASTNode) grouping.getChild(1);
            boolean changed = false;

            int gpsize = gb.getChildCount();
            for (int gbi = 0; gbi < gpsize; gbi++) {
              ASTNode gbinode = (ASTNode) gb.getChild(gbi);

              if (gbinode.toStringTree().equalsIgnoreCase(
                  groupingPara.toStringTree())) {
                gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                    HiveParser.Number, "0")));
                changed = true;
                break;
              }

            }

            if (!changed) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "1")));
            }
          }

        } else {
          for (GroupingPosition gp : gps) {

            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }
      }

    }

    ASTNode sonUnion = trees.get(0);

    for (int i = 1; i < size; i++) {
      ASTNode fatherUnion = new ASTNode(new CommonToken(HiveParser.TOK_UNION,
          "TOK_UNION"));
      fatherUnion.addChild(sonUnion);
      fatherUnion.addChild(trees.get(i));
      sonUnion = fatherUnion;
    }

    union.setChild(idx, sonUnion);
  }

  private void translateRollupAndCubeInUnionWithSelect(ASTNode union, int idx,
      GroupByInfo info, ArrayList<ASTNode> newGroupBys)
      throws SemanticException {
    int size = newGroupBys.size();
    ArrayList<ASTNode> trees = new ArrayList<ASTNode>();

    ArrayList<String> allPossibleGBs = new ArrayList<String>();
    ASTNode theFirst = newGroupBys.get(0);
    for (int i = 0; i < theFirst.getChildCount(); i++) {
      allPossibleGBs.add(((ASTNode) theFirst.getChild(i)).toStringTree()
          .toLowerCase());
    }
    ArrayList<String> inRollupAndCube = info.getInRollupAndCube();

    for (int i = 0; i < size; i++) {
      ASTNode newTree = new ASTNode(new CommonToken(HiveParser.TOK_QUERY,
          "TOK_QUERY"));
      trees.add(newTree);

      newTree.addChild(info.getFrom());

      newTree.addChild(new ASTNode(new CommonToken(HiveParser.TOK_INSERT,
          "TOK_INSERT")));

      ASTNode insert = (ASTNode) newTree.getChild(1);
      int s = 0;

      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_DESTINATION,
          "TOK_DESTINATION")));
      ASTNode subDest = (ASTNode) insert.getChild(0);
      subDest.addChild(new ASTNode(new CommonToken(HiveParser.TOK_DIR,
          "TOK_DIR")));
      ASTNode subDir = (ASTNode) subDest.getChild(0);
      subDir.addChild(new ASTNode(new CommonToken(HiveParser.TOK_TMP_FILE,
          "TOK_TMP_FILE")));
      s++;

      insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
          "TOK_SELECT")));
      s++;

      ASTNode select = (ASTNode) insert.getChild(s - 1);
      ArrayList<GroupingPosition> gps = new ArrayList<GroupingPosition>();
      ArrayList<ASTNode> sels = info.getSelect();
      int j = 0;

      for (ASTNode sel : sels) {
        ASTNode newSel = dupASTNode(sel);

        select.addChild(newSel);
        j++;
      }

      if (info.getWhere() != null) {
        insert.addChild(info.getWhere());
        s++;
      }

      findAllGrouping(select, gps);

      ASTNode gb = newGroupBys.get(i);
      if (gb.getChildCount() != 0) {
        insert.addChild(gb);
        s++;
      }

      else
        gb = null;

      ArrayList<String> allCurrentGBs = new ArrayList<String>();
      if (gb != null) {
        for (int k = 0; k < gb.getChildCount(); k++) {
          allCurrentGBs.add(((ASTNode) gb.getChild(k)).toStringTree()
              .toLowerCase());
        }
      }

      HashSet<String> toConstitutes = new HashSet<String>();
      for (int k = 0; k < select.getChildCount(); k++) {
        String gbstring = ((ASTNode) select.getChild(k)).toStringTree()
            .toLowerCase();
        toConstitutes.clear();
        boolean containsGrouping = gbstring.contains("(tok_function grouping");

        for (String test : allPossibleGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.add(test);

          }
        }

        for (String test : allCurrentGBs) {
          if (gbstring.contains(test)) {
            toConstitutes.remove(test);

          }
        }

        if (!toConstitutes.isEmpty() && !containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);

          if(cubeNullFilterEnable){
            translateColToNull(toChange);
          }
          else{
            toChange.setChild(0, new ASTNode(new CommonToken(HiveParser.TOK_NULL,
                "TOK_NULL")));
          }
          translateColToNull(toChange);

        } else if (!toConstitutes.isEmpty() && containsGrouping) {
          ASTNode toChange = (ASTNode) select.getChild(k);

          for (String toConstitute : toConstitutes) {
            constituteDescendant(toChange, toConstitute);
          }
        }
      }

      if (gb != null) {
        for (GroupingPosition gp : gps) {
          ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
          assert grouping.getChildCount() == 2;
          ASTNode groupingPara = (ASTNode) grouping.getChild(1);
          boolean changed = false;
          boolean inAll = false;
          String groupingParaStr = groupingPara.toStringTree().toLowerCase();

          for (String test : inRollupAndCube) {
            if (test.equals(groupingParaStr)) {
              inAll = true;
              break;
            }
          }
          if (!inAll) {
            throw new SemanticException("The grouping parameter: "
                + ((ASTNode) groupingPara.getChild(0)).toStringTree()
                + " is not a parameter of rollup or cube!");
          }

          int gpsize = gb.getChildCount();
          for (int gbi = 0; gbi < gpsize; gbi++) {
            ASTNode gbinode = (ASTNode) gb.getChild(gbi);

            if (gbinode.toStringTree().equalsIgnoreCase(
                groupingPara.toStringTree())) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "0")));
              changed = true;
              break;
            }

          }

          if (!changed) {
            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }

      } else {
        for (GroupingPosition gp : gps) {

          gp.father.setChild(gp.position, new ASTNode(new CommonToken(
              HiveParser.Number, "1")));
        }

        insert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_GROUPBY,
            "TOK_GROUPBY")));
        s++;
        ASTNode pseudoGroupBy = (ASTNode) insert.getChild(s - 1);
        pseudoGroupBy.addChild(new ASTNode(new CommonToken(HiveParser.Number,
            "0")));
      }

      if (info.getHaving() != null) {
        ASTNode newHaving = dupASTNode(info.getHaving());
        insert.addChild(newHaving);
        s++;

        gps.clear();
        findAllGrouping(newHaving, gps);

        if (gb != null) {
          for (GroupingPosition gp : gps) {
            ASTNode grouping = (ASTNode) gp.father.getChild(gp.position);
            assert grouping.getChildCount() == 2;
            ASTNode groupingPara = (ASTNode) grouping.getChild(1);
            boolean changed = false;

            int gpsize = gb.getChildCount();
            for (int gbi = 0; gbi < gpsize; gbi++) {
              ASTNode gbinode = (ASTNode) gb.getChild(gbi);

              if (gbinode.toStringTree().equalsIgnoreCase(
                  groupingPara.toStringTree())) {
                gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                    HiveParser.Number, "0")));
                changed = true;
                break;
              }
            }

            if (!changed) {
              gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                  HiveParser.Number, "1")));
            }
          }

        } else {
          for (GroupingPosition gp : gps) {

            gp.father.setChild(gp.position, new ASTNode(new CommonToken(
                HiveParser.Number, "1")));
          }
        }
      }

    }

    ASTNode sonUnion = trees.get(0);

    for (int i = 1; i < size; i++) {
      ASTNode fatherUnion = new ASTNode(new CommonToken(HiveParser.TOK_UNION,
          "TOK_UNION"));
      fatherUnion.addChild(sonUnion);
      fatherUnion.addChild(trees.get(i));
      sonUnion = fatherUnion;
    }

    ASTNode query = new ASTNode(new CommonToken(HiveParser.TOK_QUERY,
        "TOK_QUERY"));
    union.setChild(idx, query);

    query
        .addChild(new ASTNode(new CommonToken(HiveParser.TOK_FROM, "TOK_FROM")));
    ASTNode subFrom = (ASTNode) query.getChild(0);
    subFrom.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY,
        "TOK_SUBQUERY")));
    ASTNode subQuery = (ASTNode) subFrom.getChild(0);

    subQuery.addChild(sonUnion);

    Random rand = new Random();
    String queryName = "union_result_" + rand.nextInt(10) + rand.nextInt(10)
        + rand.nextInt(10) + rand.nextInt(10);
    subQuery.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
        queryName)));

    query.addChild(new ASTNode(new CommonToken(HiveParser.TOK_INSERT,
        "TOK_INSERT")));
    ASTNode subInsert = (ASTNode) query.getChild(1);

    subInsert.addChild(info.getDestination());

    subInsert.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELECT,
        "TOK_SELECT")));
    ASTNode subSelect = (ASTNode) subInsert.getChild(1);
    subSelect.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR,
        "TOK_SELEXPR")));
    ASTNode subSelexpr = (ASTNode) subSelect.getChild(0);
    subSelexpr.addChild(new ASTNode(new CommonToken(HiveParser.TOK_ALLCOLREF,
        "TOK_ALLCOLREF")));

    if (info.getOtherInsertNodes() != null) {
      int subi = 2;
      for (ASTNode node : info.getOtherInsertNodes()) {
        subInsert.addChild(node);
        subi++;
      }
    }
  }
}
