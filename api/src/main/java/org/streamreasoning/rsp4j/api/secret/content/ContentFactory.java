package org.streamreasoning.rsp4j.api.secret.content;

public interface ContentFactory<T1, T2, T3> {

    Content<T1, T2, T3> createEmpty();

    Content<T1, T2, T3> create();


}
