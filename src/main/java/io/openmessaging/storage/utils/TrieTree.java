/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CopyOnWriteArrayList;

public class TrieTree<T> {

    private Node root;

    public TrieTree(){
        this.root = new Node();
    }

    public void put(String key,T data){
        if(key==null||key.isEmpty()){
            return;
        }

        Node ptr = root;
        char[] keyChar = key.toCharArray();
        int keyCharIndex = 0 ;

        while (ptr!=null && keyCharIndex < keyChar.length){
           ptr = ptr.getOrCreateChildNode(keyChar[keyCharIndex ++]);
        }

        if(ptr!=null){
            ptr.setData(data);
        }

    }

    public T get(String key){

        if(key==null||key.isEmpty()){
            return null;
        }

        Node ptr = root;
        char[] keyChar = key.toCharArray();
        int keyCharIndex = 0 ;

        while (ptr!=null && keyCharIndex < keyChar.length){
            ptr = ptr.getChildNode(keyChar[keyCharIndex ++]);
        }

        if(ptr !=null){
            return (T) ptr.getData();
        }else {
            return null;
        }

    }

    private static class Node<T> {

        private char curChar;
        private T data;
        private CopyOnWriteArrayList<Node> child;

        public Node() {
            this.child = new CopyOnWriteArrayList<>();
        }

        public Node(char curChar) {
            this.curChar = curChar;
            this.child = new CopyOnWriteArrayList<>();
        }

        public void setData(T data){
            this.data = data;
        }

        public T getData() {
            return data;
        }

        public Node getOrCreateChildNode(char nextChar){
            Node childNode = null;
            if(child.isEmpty()){
                childNode = new Node(nextChar);
                child.add(childNode);
            }else{
                childNode = getChildNode(nextChar);
                if(childNode==null){
                    childNode = new Node(nextChar);
                    child.add(childNode);
                }
            }
            return childNode;
        }

        public Node getChildNode(char nextChar){
            Iterator<Node> iterator = child.iterator();
            while (iterator.hasNext()){
                Node node = iterator.next();
                if(node.curChar == nextChar){
                    return node;
                }
            }
            return null;
        }

    }



}