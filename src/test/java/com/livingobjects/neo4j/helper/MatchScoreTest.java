package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

public class MatchScoreTest {

    @Test
    public void should_sort_one_match_score() throws Exception {
        MatchScore score0 = new MatchScore(0, 0, 0);

        TreeSet<MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score0
        ));
        assertThat(tested).containsExactly(
                score0
        );
    }

    @Test
    public void should_sort_between_two_match_scores() throws Exception {
        MatchScore score0 = new MatchScore(0, 0, 0);
        MatchScore score1 = new MatchScore(1, 0, 0);

        TreeSet<MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score0, score1
        ));
        assertThat(tested).containsExactly(
                score1,
                score0
        );
    }

    @Test
    public void should_sort_between_three_match_scores() throws Exception {
        MatchScore score0 = new MatchScore(0, 0, 0);
        MatchScore score1 = new MatchScore(1, 0, 0);
        MatchScore score2 = new MatchScore(1, 1, 0);

        TreeSet<MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score1, score2, score0
        ));
        assertThat(tested).containsExactly(
                score2, score1, score0
        );
    }

    @Test
    public void should_sort_between_three_match_scores2() throws Exception {
        MatchScore score0 = new MatchScore(0, 0, 0);
        MatchScore score1 = new MatchScore(1, 0, 0);
        MatchScore score2 = new MatchScore(1, 1, 0);
        MatchScore score21 = new MatchScore(1, 1, 0);
        MatchScore score3 = new MatchScore(1, 1, 1);

        TreeSet<MatchScore> tested = Sets.newTreeSet();
        tested.addAll(ImmutableSet.of(
                score1, score2, score0, score21, score3
        ));
        assertThat(tested).containsExactly(
                score2, score3, score1, score0
        );
    }

}