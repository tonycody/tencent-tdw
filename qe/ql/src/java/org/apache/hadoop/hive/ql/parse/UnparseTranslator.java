/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.TokenRewriteStream;

import org.apache.hadoop.hive.ql.metadata.HiveUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

class UnparseTranslator {
  private final NavigableMap<Integer, Translation> translations;
  private final List<CopyTranslation> copyTranslations;
  private final NavigableMap<Integer, Translation> insertTranslation;
  private boolean enabled;

  public UnparseTranslator() {
    translations = new TreeMap<Integer, Translation>();
    copyTranslations = new ArrayList<CopyTranslation>();
    insertTranslation = new TreeMap<Integer, Translation>();
  }

  void enable() {
    enabled = true;
  }

  boolean isEnabled() {
    return enabled;
  }

  void addTranslation(ASTNode node, String replacementText) {
    if (!enabled) {
      return;
    }

    if (node.getOrigin() != null) {
      return;
    }

    int tokenStartIndex = node.getTokenStartIndex();
    int tokenStopIndex = node.getTokenStopIndex();

    Translation translation = new Translation();
    translation.tokenStopIndex = tokenStopIndex;
    translation.replacementText = replacementText;

    assert (tokenStopIndex >= tokenStartIndex);
    Map.Entry<Integer, Translation> existingEntry;
    existingEntry = translations.floorEntry(tokenStartIndex);
    if (existingEntry != null) {
      if (existingEntry.getKey() == tokenStartIndex) {
        if (existingEntry.getValue().tokenStopIndex == tokenStopIndex) {
          if (existingEntry.getValue().replacementText.equals(replacementText)) {
            return;
          }
        }
      }
      assert (existingEntry.getValue().tokenStopIndex < tokenStartIndex);
    }
    existingEntry = translations.ceilingEntry(tokenStartIndex);
    if (existingEntry != null) {
      assert (existingEntry.getKey() > tokenStopIndex);
    }

    translations.put(tokenStartIndex, translation);
  }

  void addIdentifierInsertTranslation(ASTNode identifier) {
    if (!enabled) {
      return;
    }

    assert (identifier.getToken().getType() == HiveParser.Identifier);
    String replacementText = identifier.getText();
    replacementText = BaseSemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = HiveUtils.unparseIdentifier(replacementText);
    addInsertTranslation(identifier, replacementText);
  }

  void addIdentifierInsertTranslation(ASTNode identifier, String text) {
    if (!enabled) {
      return;
    }
    assert (identifier.getToken().getType() == HiveParser.Identifier);
    String replacementText = identifier.getText();

    replacementText = BaseSemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = HiveUtils.unparseIdentifier(replacementText);
    addInsertTranslation(identifier, replacementText);
  }

  void addInsertTranslation(ASTNode node, String replacementText) {
    if (!enabled) {
      return;
    }

    if (node.getOrigin() != null) {
      return;
    }

    int tokenStartIndex = node.getTokenStartIndex();
    int tokenStopIndex = node.getTokenStopIndex();

    Translation translation = new Translation();
    translation.tokenStopIndex = tokenStopIndex;
    translation.replacementText = replacementText;

    Map.Entry<Integer, Translation> existingEntry;
    existingEntry = insertTranslation.floorEntry(tokenStartIndex);

    if (existingEntry != null) {
      if (existingEntry.getKey() == tokenStartIndex) {
        if (existingEntry.getValue().tokenStopIndex == tokenStopIndex) {
          if (existingEntry.getValue().replacementText.equals(replacementText)) {
            return;
          }
        }
      }
    }

    existingEntry = insertTranslation.ceilingEntry(tokenStartIndex);

    insertTranslation.put(tokenStartIndex, translation);
  }

  void addIdentifierTranslation(ASTNode identifier) {
    if (!enabled) {
      return;
    }
    assert (identifier.getToken().getType() == HiveParser.Identifier);
    String replacementText = identifier.getText();
    replacementText = BaseSemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = HiveUtils.unparseIdentifier(replacementText);
    addTranslation(identifier, replacementText);
  }

  void addIdentifierTranslation(ASTNode identifier, String db) {
    if (!enabled) {
      return;
    }
    assert (identifier.getToken().getType() == HiveParser.Identifier);
    String replacementText = identifier.getText();
    replacementText = BaseSemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = HiveUtils.unparseIdentifier(replacementText);
    addTranslation(identifier, replacementText);
  }

  void addCopyTranslation(ASTNode targetNode, ASTNode sourceNode) {
    if (!enabled)
      return;

    if (targetNode.getOrigin() != null)
      return;

    CopyTranslation copyTranslation = new CopyTranslation();
    copyTranslation.targetNode = targetNode;
    copyTranslation.sourceNode = sourceNode;
    copyTranslations.add(copyTranslation);

  }

  private static class CopyTranslation {
    ASTNode targetNode;
    ASTNode sourceNode;
  }

  void applyTranslation(TokenRewriteStream tokenRewriteStream) {
    for (Map.Entry<Integer, Translation> entry : translations.entrySet()) {
      tokenRewriteStream.replace(entry.getKey(),
          entry.getValue().tokenStopIndex, entry.getValue().replacementText);
    }

    for (Map.Entry<Integer, Translation> entry : insertTranslation.entrySet()) {
      tokenRewriteStream.insertBefore(entry.getKey(),
          entry.getValue().replacementText);
    }

    for (CopyTranslation copyTranslation : copyTranslations) {
      String replacementText = tokenRewriteStream.toString(
          copyTranslation.sourceNode.getTokenStartIndex(),
          copyTranslation.sourceNode.getTokenStopIndex());
      String currentText = tokenRewriteStream.toString(
          copyTranslation.targetNode.getTokenStartIndex(),
          copyTranslation.targetNode.getTokenStopIndex());
      if (currentText.equals(replacementText)) {
        continue;
      }
      addTranslation(copyTranslation.targetNode, replacementText);
      tokenRewriteStream.replace(
          copyTranslation.targetNode.getTokenStartIndex(),
          copyTranslation.targetNode.getTokenStopIndex(), replacementText);
    }

  }

  private static class Translation {
    int tokenStopIndex;
    String replacementText;

    public String toString() {
      return "" + tokenStopIndex + " -> " + replacementText;
    }
  }
}
