(function() {var implementors = {};
implementors["streams_orderednogaps"] = [{text:"impl&lt;S, F, K&gt; <a class=\"trait\" href=\"https://docs.rs/futures/0.1/futures/stream/trait.Stream.html\" title=\"trait futures::stream::Stream\">Stream</a> for <a class=\"struct\" href=\"streams_orderednogaps/struct.OrderedNoGaps.html\" title=\"struct streams_orderednogaps::OrderedNoGaps\">OrderedNoGaps</a>&lt;S, F, K&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;S: <a class=\"trait\" href=\"https://docs.rs/futures/0.1/futures/stream/trait.Stream.html\" title=\"trait futures::stream::Stream\">Stream</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;S::<a class=\"type\" href=\"https://docs.rs/futures/0.1/futures/stream/trait.Stream.html#associatedtype.Item\" title=\"type futures::stream::Stream::Item\">Item</a>: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.Ord.html\" title=\"trait core::cmp::Ord\">Ord</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;F: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/function/trait.Fn.html\" title=\"trait core::ops::function::Fn\">Fn</a>(&amp;S::<a class=\"type\" href=\"https://docs.rs/futures/0.1/futures/stream/trait.Stream.html#associatedtype.Item\" title=\"type futures::stream::Stream::Item\">Item</a>) -&gt; K,<br>&nbsp;&nbsp;&nbsp;&nbsp;K: <a class=\"trait\" href=\"streams_orderednogaps/trait.Successor.html\" title=\"trait streams_orderednogaps::Successor\">Successor</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/cmp/trait.PartialEq.html\" title=\"trait core::cmp::PartialEq\">PartialEq</a>,&nbsp;</span>",synthetic:false,types:["streams_orderednogaps::OrderedNoGaps"]},];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()
